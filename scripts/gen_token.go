// Gera um JWT HS256 de teste no formato emitido pelo os-service (para o Postman e os scripts).
//
//	go run ./scripts/gen_token.go -role mechanic -sub mec-1
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func main() {
	secret := flag.String("secret", envOr("JWT_SECRET", "change-me-in-production"), "segredo HS256 (JWT_SECRET)")
	role := flag.String("role", "mechanic", "admin | mechanic | client")
	sub := flag.String("sub", "mecanico-postman", "id do usuário (claim sub)")
	ttl := flag.Duration("ttl", 24*time.Hour, "validade do token")
	flag.Parse()

	claims := jwt.MapClaims{
		"sub":   *sub,
		"role":  *role,
		"name":  "Usuário de teste",
		"email": *sub + "@oficina.local",
		"exp":   time.Now().Add(*ttl).Unix(),
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(*secret))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(signed)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
