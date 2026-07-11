package imap

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

type MockIMAPServer struct {
	listener net.Listener
	addr     string
	mu       sync.Mutex
	conns    []net.Conn
	done     chan struct{}
}

func NewMockIMAPServer() (*MockIMAPServer, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return &MockIMAPServer{
		listener: l,
		addr:     l.Addr().String(),
		done:     make(chan struct{}),
	}, nil
}

func (s *MockIMAPServer) Addr() string {
	return s.addr
}

func (s *MockIMAPServer) Start() {
	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			go s.handleConn(conn)
		}
	}()
}

func (s *MockIMAPServer) Close() {
	s.listener.Close()
	s.mu.Lock()
	for _, conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()
}

func (s *MockIMAPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	// Welcome banner
	writer.WriteString("* OK Mock IMAP Server Ready\r\n")
	writer.Flush()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			continue
		}

		tag := parts[0]
		cmd := strings.ToUpper(parts[1])

		switch cmd {
		case "CAPABILITY":
			writer.WriteString("* CAPABILITY IMAP4rev1 IDLE\r\n")
			writer.WriteString(fmt.Sprintf("%s OK CAPABILITY completed\r\n", tag))
		case "LOGIN":
			writer.WriteString(fmt.Sprintf("%s OK LOGIN completed\r\n", tag))
		case "SELECT":
			writer.WriteString("* 1 EXISTS\r\n")
			writer.WriteString("* 0 RECENT\r\n")
			writer.WriteString(fmt.Sprintf("%s OK [READ-WRITE] SELECT completed\r\n", tag))
		case "IDLE":
			writer.WriteString("+ idling\r\n")
			writer.Flush()

			// Loop until DONE
			for {
				idleLine, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				idleLine = strings.TrimSpace(idleLine)
				if strings.ToUpper(idleLine) == "DONE" {
					writer.WriteString(fmt.Sprintf("%s OK IDLE completed\r\n", tag))
					break
				}
			}
		case "NOOP":
			writer.WriteString(fmt.Sprintf("%s OK NOOP completed\r\n", tag))
		case "FETCH":
			// Return mock envelope and bodystructure
			writer.WriteString("* 1 FETCH (ENVELOPE (\"Wed, 08 Jul 2026 11:00:00 +0700\" \"Netflix OTP code\" ((nil nil \"info\" \"netflix.com\")) ((nil nil \"info\" \"netflix.com\")) ((nil nil \"info\" \"netflix.com\")) ((nil nil \"recipient\" \"example.com\")) nil nil nil \"<message-id>\") BODYSTRUCTURE (\"text\" \"html\" (\"charset\" \"utf-8\") nil nil \"quoted-printable\" 50 2 nil nil nil nil))\r\n")
			// Return mock body when fetching body
			if len(parts) > 2 && strings.Contains(parts[2], "BODY[") {
				writer.WriteString("* 1 FETCH (BODY[1] {41}\r\nYour Netflix verification code is 884712.\r\n)\r\n")
			}
			writer.WriteString(fmt.Sprintf("%s OK FETCH completed\r\n", tag))
		case "LOGOUT":
			writer.WriteString("* BYE\r\n")
			writer.WriteString(fmt.Sprintf("%s OK LOGOUT completed\r\n", tag))
			writer.Flush()
			return
		default:
			writer.WriteString(fmt.Sprintf("%s BAD Unknown command\r\n", tag))
		}
		writer.Flush()
	}
}
