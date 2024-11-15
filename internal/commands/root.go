package commands

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var rootCommand = &cobra.Command{
	Use:   "plato-cli",
	Short: "A key-value database based on lsm-tree",
	Long:  "A key-value database based on lsm-tree",
	Run: func(cmd *cobra.Command, args []string) {

		server, _ := cmd.Flags().GetString("server")
		conn, err := ConnectToServer(server)
		if err != nil {
			fmt.Println("连接错误:", err)
			return
		}
		defer conn.Close()

		// 认证命令
		// if password != "" {
		// 	authResponse, err := SendCommand(conn, "AUTH "+password)
		// 	if err != nil {
		// 		fmt.Println("认证错误:", err)
		// 		return
		// 	}
		// 	fmt.Println("认证响应:", authResponse)
		// }

		// 启动命令处理循环
		HandleCommandLoop(conn)

		// reader := bufio.NewReader(os.Stdin)

		// for {
		// 	fmt.Print("> ")
		// 	input, _ := reader.ReadString('\n')
		// 	input = strings.TrimSpace(input)

		// 	if input == "exit" {
		// 		break
		// 	}

		// 	// 发送命令到服务器
		// 	fmt.Fprintf(conn, "%s\n", input)

		// 	// 读取响应
		// 	response, err := bufio.NewReader(conn).ReadString('\n')
		// 	if err != nil {
		// 		fmt.Println("Error reading from server:", err)
		// 		break
		// 	}
		// 	fmt.Print(response)
		// }
	},
}

func Execute() {

	rootCommand.PersistentFlags().String("server", "127.0.0.1:6399", "服务器地址和端口")
	if err := rootCommand.Execute(); err != nil {
		log.Fatal(err)
	}
}
