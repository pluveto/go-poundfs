package main

import (
	"flag"
	"sync"

	"github.com/hanwen/go-fuse/v2/fuse"
	log "github.com/sirupsen/logrus"
)

func init() {
	stdFormatter := &log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "15:04:05.000000",
		ForceColors:     true,
		DisableColors:   false,
	}
	log.SetFormatter(stdFormatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n\tpoundfs MOUNTPOINT")
	}
	// *debug = true
	if *debug {
		log.SetLevel(log.DebugLevel)
		log.Warn("Debug mode enabled")
	}
	// use device.bin as dev
	fname := "./device.bin"
	// get file size
	fsize, err := GetFileSize(fname)
	if err != nil {
		log.Fatal(err)
	}

	blockcount := uint64(fsize / 512)
	dev, err := NewFileBlockDevice(fname, blockcount)
	if err != nil {
		panic(err)
	}
	if dev == nil {
		panic("dev is nil")
	}
	fs := NewPoundFS(dev)
	if nil == fs {
		panic("fs is nil")
	}
	fs.SetDebug(*debug)
	mountpoint := flag.Args()[0]
	server, err := fuse.NewServer(fs, mountpoint, &fuse.MountOptions{RememberInodes: true, Debug: *debug})
	if err != nil {
		panic(err)
	}
	server.SetDebug(*debug)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		server.Serve()
		defer server.Unmount()
		defer wg.Done()
	}()

	if err := server.WaitMount(); err != nil {
		panic(err)
	}

	wg.Wait()
}
