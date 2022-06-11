package main

func Blue(s string) string {
	return "\033[34m" + s + "\033[0m"
}

func Red(s string) string {
	return "\033[31m" + s + "\033[0m"
}

func Green(s string) string {
	return "\033[32m" + s + "\033[0m"
}

func Yellow(s string) string {
	return "\033[33m" + s + "\033[0m"
}
