package commands

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// ConnectToServer attempts to establish a TCP connection to the specified server address within 5 seconds.
// It returns a net.Conn if successful, along with an error which is nil if no error occurred.
func ConnectToServer(address string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("无法连接到服务器: %v", err)
	}
	return conn, nil
}

// SendCommand sends a command to the server over the provided connection formatted as a Redis protocol message and returns the server's response.
// Args:
//
//	conn: The network connection to send the command through.
//	command: The command string to be sent to the server.
//
// Returns:
//
//	A string representing the server's response and an error if any occurred during sending or receiving.
func SendCommand(conn net.Conn, command string) (string, error) {
	// 拆分命令和参数
	parts := strings.Fields(command)
	// 封装为 RESP 协议的数组格式
	resp := fmt.Sprintf("*%d\r\n", len(parts))
	for _, part := range parts {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(part), part)
	}

	// 发送命令
	_, err := conn.Write([]byte(resp))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	// 读取服务器响应
	reader := bufio.NewReader(conn)
	response, err := readResp(reader)
	if err != nil {
		return "", fmt.Errorf("接收响应失败: %v", err)
	}
	return response, nil
}

// HandleCommandLoop continuously prompts the user for commands, sends them to the server using the SendCommand function,
// and displays the server's response. The loop exits when the user inputs "exit".
// It uses os.Stdin for reading commands and the provided net.Conn for communication.
func HandleCommandLoop(conn net.Conn) {
	reader := bufio.NewReader(os.Stdin)
	for {

		// 提示用户输入命令
		fmt.Print(conn.RemoteAddr().String() + ">")
		command, _ := reader.ReadString('\n')
		command = strings.TrimSpace(command)

		// 如果用户输入的是 exit，则退出
		if command == "exit" {
			fmt.Println("退出连接")
			return
		}

		// 发送命令并获取响应

		response, err := SendCommand(conn, command)
		if err != nil {
			fmt.Println("(error):", err)
		} else {
			fmt.Println("(ok):", response)
		}
	}
}

// readResp reads and parses a RESP (REdis Serialization Protocol) message from the given reader.
// It returns a string representation of the response and an error if any occurs during reading.
func readResp(reader *bufio.Reader) (string, error) {
	// 读取 RESP 响应的第一个字符
	respType, err := reader.ReadByte()
	if err != nil {
		return "", err
	}

	// 根据响应的类型进行不同的处理
	switch respType {
	case '+': // 简单字符串
		return readSimpleString(reader)
	case '-': // 错误
		return readError(reader)
	case ':': // 整数
		return readInteger(reader)
	case '$': // 批量字符串
		return readBulkString(reader)
	case '*': // 数组
		return readArray(reader)
	default:
		return "", errors.New("不支持的响应类型")
	}
}

// readSimpleString reads a simple string from the provided reader until a newline character is encountered.
// It trims the trailing newline and spaces, then returns the resulting string along with any read error.
func readSimpleString(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

// readError reads an error message from the provided reader until a newline character is encountered.
// It prefixes the error message with "ERROR: " and trims trailing whitespace.
// Returns the formatted error message and any read error encountered.
func readError(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return "ERROR: " + strings.TrimSpace(line), nil
}

// readInteger reads a line from the provided reader expecting an integer in Redis Serialization Protocol format.
// It returns the read line including the newline character and an error if any occurs during the read operation.
func readInteger(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line, nil
}

// readBulkString reads a bulk string from the provided reader based on the Redis Serialization Protocol.
// It returns the content of the bulk string and an error if any occurs during the read process.
func readBulkString(reader *bufio.Reader) (string, error) {
	// 读取批量字符串的长度
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	length, err := strconv.Atoi(line)
	if err != nil {
		return "", err
	}

	// 读取实际的字符串内容
	buf := make([]byte, length+2) // 额外的2字节是用于处理 \r\n
	_, err = reader.Read(buf)
	if err != nil {
		return "", err
	}

	// 返回去掉结尾的 \r\n 的内容
	return string(buf[:length]), nil
}

// readArray reads an array from the provided reader based on the Redis Serialization Protocol.
// It first reads the count of elements, then iterates to read each element using readResp.
// The function returns a concatenated string of array elements separated by newlines and an error if any.
func readArray(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)
	count, err := strconv.Atoi(line)
	if err != nil {
		return "", err
	}

	// 读取数组元素
	var result string
	for i := 0; i < count; i++ {
		// 读取每个元素
		element, err := readResp(reader)
		if err != nil {
			return "", err
		}
		result += element + "\n"
	}

	return result, nil
}
