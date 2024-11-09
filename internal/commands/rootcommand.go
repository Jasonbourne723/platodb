package commands

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var rootCommand = &cobra.Command{
	Use:   "plato-cli",
	Short: "A key-value database based on lsm-tree",
	Long:  "A key-value database based on lsm-tree",
	Run: func(cmd *cobra.Command, args []string) {
		interactiveMode()
	},
}

var (
	host string
	port int
)

func Execute() {

	rootCommand.Flags().StringVarP(&host, "host", "H", "127.0.0.1", "host of server")
	rootCommand.Flags().IntVarP(&port, "port", "p", 6379, "port of server")

	if err := rootCommand.Execute(); err != nil {
		log.Fatal(err)
	}
}

// func login() error {
// 	serverAddr := fmt.Sprintf("%s:%d", host, port)
// 	conn, err := net.Dial("tcp", serverAddr)
// 	if err != nil {
// 		return err
// 	}
// 	defer conn.Close()

// 	// 读取响应
// 	response, err := bufio.NewReader(conn).ReadString('\n')
// 	if err != nil {
// 		return err
// 	}
// 	if strings.HasPrefix(response, "-ERR") {
// 		return fmt.Errorf("authentication failed")
// 	}
// 	return nil
// }

func interactiveMode() {
	serverAddr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	} else {
		fmt.Println("connected successful")
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "exit" {
			break
		}

		// 发送命令到服务器
		fmt.Fprintf(conn, "%s\n", input)

		// 读取响应
		response, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("Error reading from server:", err)
			break
		}
		fmt.Print(response)
	}
}
