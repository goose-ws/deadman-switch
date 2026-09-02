package main

import (
	crand "crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/smtp"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// --- Configuration ---
type Config struct {
	NodeID    string   `json:"node_id"`
	Address   string   `json:"address"`
	Peers     []string `json:"peers"`
	DataDir   string   `json:"data_dir"`
	CertFile  string   `json:"cert_file"`
	KeyFile   string   `json:"key_file"`
	CACert    string   `json:"ca_cert,omitempty"`
	Duration  string   `json:"duration,omitempty"`

	TelegramToken     string  `json:"telegram_token"`
	PayloadPath       string  `json:"payload_path"`
	TelegramChatID    int64   `json:"telegram_chat_id,omitempty"`
	TelegramThreadID  int64   `json:"telegram_thread_id,omitempty"`
	AnnounceTopology  bool    `json:"announce_topology,omitempty"`
	AnswerOffset      int     `json:"answer_offset"`
	CommunicationTime int     `json:"communication_time,omitempty"`
	AuthorizedUserIDs []int64 `json:"authorized_user_ids,omitempty"`

	DisablePassphrase string `json:"disable_passphrase,omitempty"`

	SMTPHost        string `json:"smtp_host,omitempty"`
	SMTPPort        int    `json:"smtp_port,omitempty"`
	SMTPImplicitTLS bool   `json:"smtp_tls,omitempty"`
	SMTPUser        string `json:"smtp_user,omitempty"`
	SMTPPassword    string `json:"smtp_password,omitempty"`
	SMTPSender      string `json:"smtp_sender,omitempty"`
	SMTPRecipient   string `json:"smtp_recipient,omitempty"`
}

// --- State Machine ---
type DeadmanState struct {
	InitialArmCode   string `json:"initial_arm_code"`
	Deadline         int64  `json:"deadline"`
	ChatID           int64  `json:"chat_id"`
	AuthorizedUserID int64  `json:"authorized_user_id"`
	Answer           int    `json:"answer"`
	Armed            bool   `json:"armed"`
	Triggered        bool   `json:"triggered"`
	FailureCount     int    `json:"failure_count"`
	NextAttemptUnix  int64  `json:"next_attempt_unix"`
	Snoozed          bool   `json:"snoozed"`
	WakeupTime       int64  `json:"wakeup_time"`
	WarningLevel     int    `json:"warning_level"`
	OriginalDuration int64  `json:"original_duration"`

	Disabled         bool   `json:"disabled"`

	DisableFailureCount    int   `json:"disable_failure_count"`
	DisableNextAttemptUnix int64 `json:"disable_next_attempt_unix"`

	EventHistory []HistoryEvent `json:"event_history,omitempty"`

	TestSeq          int64             `json:"test_seq"`
	TestNodes        []string          `json:"test_nodes,omitempty"`
	TestCurrentIdx   int               `json:"test_current_idx"`
	TestResults      map[string]string `json:"test_results,omitempty"`
	TestInProgress   bool              `json:"test_in_progress"`
	TestNextNodeAt   int64             `json:"test_next_node_at"`
	TestReportPosted bool              `json:"test_report_posted"`
	TestTriggeredBy  string            `json:"test_triggered_by,omitempty"`
}

type DeadmanFSM struct {
	mu    sync.RWMutex
	state DeadmanState
}

func (f *DeadmanFSM) Apply(l *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	var newState DeadmanState
	if err := json.Unmarshal(l.Data, &newState); err != nil {
		log.Printf("❌ FSM Error: %v", err)
		return nil
	}
	f.state = newState
	return nil
}

func (f *DeadmanFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &fsmSnapshot{state: f.state}, nil
}

func (f *DeadmanFSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer rc.Close()
	return json.NewDecoder(rc).Decode(&f.state)
}

type fsmSnapshot struct{ state DeadmanState }

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	b, err := json.Marshal(f.state)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(b); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}
func (f *fsmSnapshot) Release() {}

// --- History ---

type HistoryEvent struct {
	Time   int64  `json:"t"`
	Event  string `json:"e"`
	NodeID string `json:"n,omitempty"`
	UserID int64  `json:"u,omitempty"`
}

func appendHistory(state *DeadmanState, event, nodeID string, userID int64) {
	state.EventHistory = append(state.EventHistory, HistoryEvent{
		Time:   time.Now().Unix(),
		Event:  event,
		NodeID: nodeID,
		UserID: userID,
	})
	if len(state.EventHistory) > 10 {
		state.EventHistory = state.EventHistory[len(state.EventHistory)-10:]
	}
}

// --- Helpers ---

func cryptoRandIntn(n int) int {
	val, err := crand.Int(crand.Reader, big.NewInt(int64(n)))
	if err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return int(val.Int64())
}

func isAuthorized(userID int64, primaryID int64, extraIDs []int64) bool {
	if userID == primaryID {
		return true
	}
	for _, id := range extraIDs {
		if userID == id {
			return true
		}
	}
	return false
}

func parseCommand(text string) (string, string) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "/") {
		return "", text
	}
	parts := strings.SplitN(text, " ", 2)
	cmd := parts[0]
	args := ""
	if len(parts) > 1 {
		args = strings.TrimSpace(parts[1])
	}
	if idx := strings.Index(cmd, "@"); idx != -1 {
		cmd = cmd[:idx]
	}
	return cmd, args
}

func newMsg(chatID int64, text string) tgbotapi.MessageConfig {
	return tgbotapi.NewMessage(chatID, text)
}

func sendMsg(bot *tgbotapi.BotAPI, conf Config, msg tgbotapi.MessageConfig) (tgbotapi.Message, error) {
	if conf.TelegramThreadID == 0 {
		return bot.Send(msg)
	}
	params := tgbotapi.Params{
		"chat_id":           strconv.FormatInt(msg.ChatID, 10),
		"text":              msg.Text,
		"message_thread_id": strconv.FormatInt(conf.TelegramThreadID, 10),
	}
	if msg.ParseMode != "" {
		params["parse_mode"] = msg.ParseMode
	}
	if msg.DisableWebPagePreview {
		params["disable_web_page_preview"] = "true"
	}
	resp, err := bot.MakeRequest("sendMessage", params)
	if err != nil {
		return tgbotapi.Message{}, err
	}
	var sent tgbotapi.Message
	if err := json.Unmarshal(resp.Result, &sent); err != nil {
		return tgbotapi.Message{}, err
	}
	return sent, nil
}

func effectiveChatID(conf Config, state DeadmanState) int64 {
	if conf.TelegramChatID != 0 {
		return conf.TelegramChatID
	}
	return state.ChatID
}

func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err != nil {
			return 0, fmt.Errorf("invalid day duration %q: %w", s, err)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

func sendSMTP(conf Config, subject, body string) error {
	if conf.SMTPHost == "" || conf.SMTPRecipient == "" {
		return nil
	}
	port := conf.SMTPPort
	if port == 0 {
		if conf.SMTPImplicitTLS {
			port = 465
		} else {
			port = 587
		}
	}
	addr := fmt.Sprintf("%s:%d", conf.SMTPHost, port)
	sender := conf.SMTPSender
	if sender == "" {
		sender = conf.SMTPUser
	}
	raw := []byte(fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s",
		sender, conf.SMTPRecipient, subject, body,
	))

	if conf.SMTPImplicitTLS {
		conn, err := tls.Dial("tcp", addr, &tls.Config{ServerName: conf.SMTPHost})
		if err != nil {
			return fmt.Errorf("smtp dial: %w", err)
		}
		client, err := smtp.NewClient(conn, conf.SMTPHost)
		if err != nil {
			return fmt.Errorf("smtp client: %w", err)
		}
		defer client.Close()
		if err := client.Auth(smtp.PlainAuth("", conf.SMTPUser, conf.SMTPPassword, conf.SMTPHost)); err != nil {
			return fmt.Errorf("smtp auth: %w", err)
		}
		if err := client.Mail(sender); err != nil {
			return fmt.Errorf("smtp MAIL: %w", err)
		}
		if err := client.Rcpt(conf.SMTPRecipient); err != nil {
			return fmt.Errorf("smtp RCPT: %w", err)
		}
		w, err := client.Data()
		if err != nil {
			return fmt.Errorf("smtp DATA: %w", err)
		}
		if _, err := w.Write(raw); err != nil {
			return fmt.Errorf("smtp write: %w", err)
		}
		return w.Close()
	}

	return smtp.SendMail(addr,
		smtp.PlainAuth("", conf.SMTPUser, conf.SMTPPassword, conf.SMTPHost),
		sender, []string{conf.SMTPRecipient}, raw)
}

func sendWarning(bot *tgbotapi.BotAPI, conf Config, chatID int64, subject, text string) {
	msg := newMsg(chatID, text)
	msg.ParseMode = "Markdown"
	sendMsg(bot, conf, msg)

	plain := strings.NewReplacer("*", "", "`", "", "_", "").Replace(text)
	if err := sendSMTP(conf, subject, plain); err != nil {
		log.Printf("⚠️ SMTP send failed: %v", err)
	}
}

// Dynamically retrieves a Telegram Bot API instance and handles failure gracefully
func initBot(token string) *tgbotapi.BotAPI {
	if token == "" {
		return nil
	}
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		log.Printf("⚠️ Telegram Init Failed: %v", err)
		return nil
	}
	return bot
}

// --- Cert Expiry Check ---

func checkCertExpiry(conf Config, state DeadmanState) {
	warnWithin := 10 * 24 * time.Hour

	certData, err := os.ReadFile(conf.CertFile)
	if err != nil {
		log.Printf("⚠️ CERT CHECK: Could not read cert file %s: %v", conf.CertFile, err)
		return
	}

	// Parse all PEM blocks to find the leaf certificate
	var leafCert *x509.Certificate
	rest := certData
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		parsed, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		leafCert = parsed
		break // first cert in chain is the leaf
	}

	if leafCert == nil {
		log.Printf("⚠️ CERT CHECK: No valid certificate found in %s", conf.CertFile)
		return
	}

	now := time.Now()
	expiry := leafCert.NotAfter
	remaining := expiry.Sub(now)

	var logMsg, tgMsg string

	if now.After(expiry) {
		logMsg = fmt.Sprintf("🚨 CERT EXPIRED: %s expired on %s (%s ago). Peers will reject connections.",
			conf.CertFile, expiry.Format("2006-01-02"), (-remaining).Round(time.Hour))
		tgMsg = fmt.Sprintf("🚨 *Certificate EXPIRED* on node `%s`\nFile: `%s`\nExpired: %s (%s ago)\nPeers will reject all connections until the certificate is renewed and the service restarted.",
			conf.NodeID, conf.CertFile, expiry.Format("2006-01-02"), (-remaining).Round(time.Hour))
	} else if remaining < warnWithin {
		logMsg = fmt.Sprintf("⚠️ CERT EXPIRING SOON: %s expires on %s (%s remaining).",
			conf.CertFile, expiry.Format("2006-01-02"), remaining.Round(time.Hour))
		tgMsg = fmt.Sprintf("⚠️ *Certificate expiring soon* on node `%s`\nFile: `%s`\nExpires: %s (%s remaining)\nRenew and restart the service before it expires.",
			conf.NodeID, conf.CertFile, expiry.Format("2006-01-02"), remaining.Round(time.Hour))
	} else {
		log.Printf("✅ CERT CHECK: %s valid until %s (%s remaining).",
			conf.CertFile, expiry.Format("2006-01-02"), remaining.Round(24*time.Hour))
		return
	}

	log.Printf("%s", logMsg)

	bot := initBot(conf.TelegramToken)
	if bot == nil {
		return
	}
	chatID := effectiveChatID(conf, state)
	if chatID == 0 {
		return
	}
	msg := tgbotapi.NewMessage(chatID, tgMsg)
	msg.ParseMode = "Markdown"
	sendMsg(bot, conf, msg)
}

// --- Main ---
func main() {
	confPath := flag.String("config", "config.json", "Path to config file")
	flag.Parse()

	cfgData, err := os.ReadFile(*confPath)
	if err != nil {
		log.Fatalf("Fatal: Could not read config: %v", err)
	}

	conf := Config{AnswerOffset: 3, CommunicationTime: 120}
	if err := json.Unmarshal(cfgData, &conf); err != nil {
		log.Fatalf("Fatal: Config parse error: %v", err)
	}
	if conf.CommunicationTime <= 0 {
		conf.CommunicationTime = 120 // Fallback if accidentally set to 0
	}
	if conf.TelegramToken == "" {
		log.Fatalf("Fatal: telegram_token is missing from config.json")
	}
	if conf.PayloadPath == "" {
		log.Fatalf("Fatal: payload_path is missing from config.json")
	}

	// Verify critical files exist before starting
	for _, file := range []string{conf.CertFile, conf.KeyFile, conf.PayloadPath} {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			log.Fatalf("Fatal: Required file missing or inaccessible: %s", file)
		}
	}

	log.Printf("🚀 Starting Deadman Switch Node: %s", conf.NodeID)

	tlsConfig, err := setupTLS(conf)
	if err != nil {
		log.Fatalf("Fatal: TLS setup error: %v", err)
	}

	os.MkdirAll(conf.DataDir, 0700)
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(conf.DataDir, "raft.db"))
	if err != nil {
		log.Fatalf("Fatal: BoltDB init error: %v", err)
	}
	snapStore, err := raft.NewFileSnapshotStore(conf.DataDir, 2, os.Stderr)
	if err != nil {
		log.Fatalf("Fatal: Snapshot store error: %v", err)
	}

	listenAddr := ":8443"
	if idx := strings.LastIndex(conf.Address, ":"); idx != -1 {
		listenAddr = ":" + conf.Address[idx+1:]
	}
	streamLay, err := newTLSStreamLayer(listenAddr, conf.Address, tlsConfig)
	if err != nil {
		log.Fatalf("Fatal: Transport setup error: %v", err)
	}

	raftLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Output: os.Stderr,
		Level:  hclog.Warn,
	})

	transport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  streamLay,
		MaxPool: 3,
		Timeout: 10 * time.Second,
		Logger:  raftLogger,
	})

	fsm := &DeadmanFSM{}
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(conf.NodeID)
	raftConfig.Logger = raftLogger
	notifyCh := make(chan bool, 1)
	raftConfig.NotifyCh = notifyCh
	raftConfig.HeartbeatTimeout = time.Duration(conf.CommunicationTime) * time.Second
	raftConfig.ElectionTimeout = time.Duration(conf.CommunicationTime) * time.Second

	r, err := raft.NewRaft(raftConfig, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		log.Fatalf("Fatal: Raft init error: %v", err)
	}

	checkCertExpiry(conf, fsm.state)

	go func() {
		for {
			select {
			case conn := <-streamLay.rpcConn:
				go handleInternalRPC(conn, r, fsm)
			case <-streamLay.closeCh:
				return
			}
		}
	}()

	hasState, _ := raft.HasExistingState(boltStore, boltStore, snapStore)
	if !hasState {
		log.Println("BOOTSTRAP: No existing state. Waiting for all peers...")
		waitForPeers(conf, tlsConfig)
		log.Println("BOOTSTRAP: Peers reachable. Bootstrapping cluster.")

		var servers []raft.Server
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(conf.NodeID),
			Address: raft.ServerAddress(conf.Address),
		})
		for _, peer := range conf.Peers {
			parts := strings.Split(peer, ":")
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(parts[0]),
				Address: raft.ServerAddress(peer),
			})
		}
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
			log.Printf("Bootstrap info: %v (safe if another node did it first)", err)
		}
	}

	go monitorLeadership(r, notifyCh, fsm, conf)
	go runControlLoop(r, fsm, conf, tlsConfig)
	go monitorClusterHealth(r, fsm, conf, tlsConfig)
	go monitorCertExpiry(conf, fsm)
	go runMonthlyTest(r, fsm, conf)
	go runTestWatcher(r, fsm, conf, tlsConfig)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 Shutting down. Waiting for Raft to safely exit...")
	r.Shutdown().Error()
	log.Println("✅ Shutdown complete.")
}

// --- Internal Test Result RPC ---
type TestResultPayload struct {
	NodeID string `json:"node_id"`
	Result string `json:"result"`
	Seq    int64  `json:"seq"`
}

func handleInternalRPC(conn net.Conn, r *raft.Raft, fsm *DeadmanFSM) {
	defer conn.Close()
	var magic [1]byte
	if _, err := io.ReadFull(conn, magic[:]); err != nil {
		return
	}
	var payload TestResultPayload
	if err := json.NewDecoder(conn).Decode(&payload); err != nil {
		return
	}

	if r.State() != raft.Leader {
		return
	}

	fsm.mu.RLock()
	latest := fsm.state
	fsm.mu.RUnlock()

	if latest.TestSeq != payload.Seq {
		return
	}

	if latest.TestResults == nil {
		latest.TestResults = make(map[string]string)
	}
	latest.TestResults[payload.NodeID] = payload.Result
	latest.TestCurrentIdx++
	latest.TestNextNodeAt = time.Now().Add(30 * time.Second).Unix()

	proposeState(r, latest)
}

// monitorCertExpiry re-runs checkCertExpiry on a schedule so the "expiring
// soon" / "expired" alerts fire even if the process happens to stay up
// (through a renewal window) for weeks between restarts - checkCertExpiry
// on its own only ever runs once, at startup.
func monitorCertExpiry(conf Config, fsm *DeadmanFSM) {
	ticker := time.NewTicker(12 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		fsm.mu.RLock()
		fsmState := fsm.state
		fsm.mu.RUnlock()
		checkCertExpiry(conf, fsmState)
	}
}

// --- Cluster Health Monitor ---
func monitorClusterHealth(r *raft.Raft, fsm *DeadmanFSM, conf Config, t *tls.Config) {
	var bot *tgbotapi.BotAPI
	peerUp := make(map[string]bool)
	for _, peer := range conf.Peers {
		peerUp[peer] = true
	}

	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		leaderAddr := string(r.Leader())
		leaderDisplay := "none"
		if leaderAddr != "" {
			leaderDisplay = leaderAddr
			if future := r.GetConfiguration(); future.Error() == nil {
				for _, srv := range future.Configuration().Servers {
					if string(srv.Address) == leaderAddr {
						leaderDisplay = string(srv.ID)
						break
					}
				}
			}
		}
		log.Printf("❤️ HEALTH CHECK: Role=%s | Leader=%s | Peers=%d",
			r.State(), leaderDisplay, len(conf.Peers)+1)

		fsm.mu.RLock()
		fsmState := fsm.state
		fsm.mu.RUnlock()
		chatID := effectiveChatID(conf, fsmState)

		for _, peer := range conf.Peers {
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", peer, t)
			isUp := err == nil
			if isUp {
				conn.Close()
				log.Printf("   ✅ Peer UP: %s", peer)
			} else {
				log.Printf("   ❌ Peer DOWN: %s (%v)", peer, err)
			}

			if wasUp := peerUp[peer]; isUp != wasUp && conf.AnnounceTopology && chatID != 0 && r.State() == raft.Leader {
				if bot == nil {
					bot = initBot(conf.TelegramToken)
				}
				if bot != nil {
					var text string
					if isUp {
						text = fmt.Sprintf("✅ *Node* `%s` *has reconnected.*", peer)
					} else {
						text = fmt.Sprintf("❌ *Node* `%s` *has disconnected.*", peer)
					}
					msg := newMsg(chatID, text)
					msg.ParseMode = "Markdown"
					sendMsg(bot, conf, msg)
				} else {
					log.Printf("⚠️ Skipping topology announcement for %s, Telegram bot not initialized.", peer)
				}
			}
			peerUp[peer] = isUp
		}
	}
}

// --- Leadership Monitor ---
func monitorLeadership(r *raft.Raft, notifyCh chan bool, fsm *DeadmanFSM, conf Config) {
	var bot *tgbotapi.BotAPI
	lastNotifiedLeader := ""

	for isLeader := range notifyCh {
		if isLeader {
			log.Printf("👑 STATE CHANGE: I have become the LEADER (%s)", conf.NodeID)
			if conf.AnnounceTopology && conf.NodeID != lastNotifiedLeader {
				fsm.mu.RLock()
				fsmState := fsm.state
				fsm.mu.RUnlock()
				chatID := effectiveChatID(conf, fsmState)
				
				if chatID != 0 {
					if bot == nil {
						bot = initBot(conf.TelegramToken)
					}
					if bot != nil {
						msg := newMsg(chatID, fmt.Sprintf(
							"⚠️ *Topology Change:* Node `%s` has assumed Leadership.", conf.NodeID))
						msg.ParseMode = "Markdown"
						sendMsg(bot, conf, msg)
					} else {
						log.Printf("⚠️ Skipping leadership announcement, Telegram bot not initialized.")
					}
				}
				lastNotifiedLeader = conf.NodeID
			}
		} else {
			log.Printf("📉 STATE CHANGE: Stepping down from Leadership.")
			lastNotifiedLeader = ""
		}
	}
}

// --- Distributed Test Coordinator ---
func initiateTest(r *raft.Raft, fsm *DeadmanFSM, conf Config, triggeredBy string) {
	if r.State() != raft.Leader {
		return
	}

	fsm.mu.RLock()
	state := fsm.state
	fsm.mu.RUnlock()

	if state.TestInProgress {
		log.Printf("⚠️ TEST ABORTED: A test is already in progress.")
		return
	}

	nodes := []string{conf.NodeID}
	nodes = append(nodes, conf.Peers...)
	for i, n := range nodes {
		if idx := strings.LastIndex(n, ":"); idx != -1 {
			nodes[i] = n[:idx]
		}
	}

	state.TestSeq++
	state.TestNodes = nodes
	state.TestCurrentIdx = 0
	state.TestResults = make(map[string]string)
	state.TestInProgress = true
	state.TestReportPosted = false
	state.TestTriggeredBy = triggeredBy
	state.TestNextNodeAt = time.Now().Unix()

	if proposeState(r, state) {
		log.Printf("🧪 TEST INITIATED (seq=%d, triggered_by=%s, nodes=%v)",
			state.TestSeq, triggeredBy, nodes)
	}
}

// Helper to chunk large log outputs and safely send them via Telegram API
func sendChunkedOutput(bot *tgbotapi.BotAPI, conf Config, chatID int64, nodeName, icon, cleanOut string) {
	header := fmt.Sprintf("🖥️ *%s* %s\n", nodeName, icon)

	if cleanOut == "" {
		msg := newMsg(chatID, header+"_(no output)_")
		msg.ParseMode = "Markdown"
		sendMsg(bot, conf, msg)
		return
	}

	// Telegram allows 4096 characters. We use 3900 to ensure we have room for the
	// Markdown formatting code blocks (```text \n ... \n```) and the node header.
	runes := []rune(cleanOut)
	maxLen := 3900

	chunkIndex := 1
	for len(runes) > 0 {
		chunkLen := maxLen
		if len(runes) < chunkLen {
			chunkLen = len(runes)
		} else {
			// Find nearest newline under the cap so we split nicely.
			foundNL := false
			for i := chunkLen - 1; i >= chunkLen-500 && i > 0; i-- { // look back up to 500 characters
				if runes[i] == '\n' {
					chunkLen = i + 1 // include the newline in the split
					foundNL = true
					break
				}
			}
			if !foundNL {
				// No newline found within 500 characters, just slice hard at maxLen
			}
		}

		chunkText := string(runes[:chunkLen])
		runes = runes[chunkLen:]

		partHeader := header
		if chunkIndex > 1 {
			partHeader = fmt.Sprintf("🖥️ *%s* %s (Part %d)\n", nodeName, icon, chunkIndex)
		}

		msgText := partHeader + "```text\n" + chunkText + "\n```"
		msg := newMsg(chatID, msgText)
		msg.ParseMode = "Markdown"

		if _, err := sendMsg(bot, conf, msg); err != nil {
			log.Printf("⚠️ FAILED TO SEND TEST CHUNK to %s: %v", nodeName, err)
		}

		chunkIndex++
	}
}

func postTestReport(bot *tgbotapi.BotAPI, conf Config, chatID int64, state DeadmanState) {
	label := "Monthly"
	if state.TestTriggeredBy == "manual" {
		label = "Manual"
	}

	overallPass := true
	for _, output := range state.TestResults {
		if strings.Contains(output, "SUMMARY:FAIL") {
			overallPass = false
			break
		}
	}

	var summary string
	if overallPass {
		summary = fmt.Sprintf("🧪 *%s Test: ✅ PASSED* (%d nodes)", label, len(state.TestResults))
	} else {
		summary = fmt.Sprintf("🧪 *%s Test: ❌ FAILED* (%d nodes)", label, len(state.TestResults))
	}

	msg := newMsg(chatID, summary)
	msg.ParseMode = "Markdown"
	if _, err := sendMsg(bot, conf, msg); err != nil {
		log.Printf("⚠️ FAILED TO POST TEST SUMMARY: %v", err)
	}

	for _, nodeName := range state.TestNodes {
		output, ok := state.TestResults[nodeName]
		if !ok {
			msg := newMsg(chatID, fmt.Sprintf("🖥️ *%s* — ⏭️ no result", nodeName))
			msg.ParseMode = "Markdown"
			sendMsg(bot, conf, msg)
			continue
		}

		nodePass := strings.Contains(output, "SUMMARY:PASS")
		icon := "✅"
		if !nodePass {
			icon = "❌"
		}

		cleanOut := strings.TrimSpace(output)
		cleanOut = strings.ReplaceAll(cleanOut, "`", "'") // sanitize backticks
		sendChunkedOutput(bot, conf, chatID, nodeName, icon, cleanOut)
	}
}

func runTestWatcher(r *raft.Raft, fsm *DeadmanFSM, conf Config, tlsConfig *tls.Config) {
	var bot *tgbotapi.BotAPI

	nodeID := conf.NodeID
	if idx := strings.LastIndex(nodeID, ":"); idx != -1 {
		nodeID = nodeID[:idx]
	}

	var lastSeenSeq int64
	var runningTest bool

	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		fsm.mu.RLock()
		state := fsm.state
		fsm.mu.RUnlock()

		if !state.TestInProgress {
			lastSeenSeq = state.TestSeq
			runningTest = false
			continue
		}

		allDone := state.TestCurrentIdx >= len(state.TestNodes)
		if allDone && !state.TestReportPosted && r.State() == raft.Leader {
			chatID := effectiveChatID(conf, state)
			reportPosted := false

			if chatID != 0 {
				if bot == nil {
					bot = initBot(conf.TelegramToken)
				}
				if bot != nil {
					postTestReport(bot, conf, chatID, state)
					log.Printf("📋 TEST REPORT POSTED (seq=%d)", state.TestSeq)
					reportPosted = true
				} else {
					log.Printf("⚠️ Could not post test report, Telegram bot not initialized. Retrying next tick.")
					continue // Block Raft state change and force retry next tick
				}
			} else {
				// No configured chat ID, just mark as posted to clear the test cycle
				reportPosted = true
			}

			if reportPosted {
				fsm.mu.RLock()
				latest := fsm.state
				fsm.mu.RUnlock()
				latest.TestReportPosted = true
				latest.TestInProgress = false
				proposeState(r, latest)
			}
			continue
		}

		// EDGE CASE: If a node dies mid-test, the test gets stuck forever.
		// The Leader enforces a 30-minute maximum execution time per node.
		if r.State() == raft.Leader && !allDone {
			deadline := state.TestNextNodeAt + 1800 // 30 minutes past allowed start time
			if time.Now().Unix() > deadline {
				stuckNode := state.TestNodes[state.TestCurrentIdx]
				log.Printf("⚠️ Node %s timed out during test execution. Force advancing...", stuckNode)

				fsm.mu.RLock()
				latest := fsm.state
				fsm.mu.RUnlock()

				if latest.TestResults == nil {
					latest.TestResults = make(map[string]string)
				}
				latest.TestResults[stuckNode] = "SUMMARY:FAIL\n[ERROR: Node timed out or disconnected during test]"
				latest.TestCurrentIdx++
				latest.TestNextNodeAt = time.Now().Add(10 * time.Second).Unix()
				proposeState(r, latest)
				continue
			}
		}

		if runningTest || state.TestCurrentIdx >= len(state.TestNodes) {
			continue
		}
		if state.TestNodes[state.TestCurrentIdx] != nodeID {
			continue
		}
		if time.Now().Unix() < state.TestNextNodeAt {
			continue
		}
		if state.TestSeq == lastSeenSeq {
			continue
		}

		runningTest = true
		lastSeenSeq = state.TestSeq
		currentSeq := state.TestSeq
		currentIdx := state.TestCurrentIdx

		log.Printf("🧪 TEST RUNNING LOCALLY (seq=%d, idx=%d/%d)",
			currentSeq, currentIdx+1, len(state.TestNodes))

		go func() {
			defer func() { runningTest = false }()

			cmd := exec.Command(conf.PayloadPath, "--test")
			output, err := cmd.CombinedOutput()
			result := string(output)
			if err != nil {
				log.Printf("⚠️ LOCAL TEST ERROR (seq=%d): %v", currentSeq, err)
				result += fmt.Sprintf("\n[exit error: %v]", err)
			}
			log.Printf("📋 LOCAL TEST DONE (seq=%d node=%s)", currentSeq, nodeID)

			if r.State() == raft.Leader {
				fsm.mu.RLock()
				latest := fsm.state
				fsm.mu.RUnlock()

				if latest.TestSeq != currentSeq {
					return
				}
				if latest.TestResults == nil {
					latest.TestResults = make(map[string]string)
				}
				latest.TestResults[nodeID] = result
				latest.TestCurrentIdx = currentIdx + 1
				latest.TestNextNodeAt = time.Now().Add(30 * time.Second).Unix()
				proposeState(r, latest)
			} else {
				leaderAddr := string(r.Leader())
				if leaderAddr != "" {
					conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", leaderAddr, tlsConfig)
					if err == nil {
						defer conn.Close()
						conn.Write([]byte{0xFF}) // Magic byte for Internal RPC
						json.NewEncoder(conn).Encode(TestResultPayload{
							NodeID: nodeID,
							Result: result,
							Seq:    currentSeq,
						})
					} else {
						log.Printf("⚠️ Failed to forward test result to leader: %v", err)
					}
				}
			}
		}()
	}
}

func runMonthlyTest(r *raft.Raft, fsm *DeadmanFSM, conf Config) {
	for {
		now := time.Now()
		next := time.Date(now.Year(), now.Month()+1, 1, 3, 0, 0, 0, now.Location())
		log.Printf("🧪 MONTHLY TEST: Next run scheduled for %s", next.Format(time.RFC1123))
		time.Sleep(time.Until(next))

		if r.State() != raft.Leader {
			continue
		}

		fsm.mu.RLock()
		fsmState := fsm.state
		fsm.mu.RUnlock()
		chatID := effectiveChatID(conf, fsmState)

		if chatID != 0 {
			if bot := initBot(conf.TelegramToken); bot != nil {
				msg := newMsg(chatID, "🧪 *Monthly system test starting across all nodes…*")
				msg.ParseMode = "Markdown"
				sendMsg(bot, conf, msg)
			}
		}

		initiateTest(r, fsm, conf, "monthly")
	}
}

// --- Main Control Loop ---
func runControlLoop(r *raft.Raft, fsm *DeadmanFSM, conf Config, tlsConfig *tls.Config) {
	timerDuration := 21 * 24 * time.Hour
	if conf.Duration != "" {
		if d, err := parseDuration(conf.Duration); err == nil {
			timerDuration = d
		} else {
			log.Printf("⚠️ Could not parse duration %q: %v — using default 21d", conf.Duration, err)
		}
	}

	var bot *tgbotapi.BotAPI
	var err error
	for {
		bot, err = tgbotapi.NewBotAPI(conf.TelegramToken)
		if err == nil {
			break
		}
		log.Printf("❌ Telegram Init Failed: %v. Retrying in 10s...", err)
		time.Sleep(10 * time.Second)
	}
	log.Printf("✅ Telegram Bot Connected: %s", bot.Self.UserName)

	lastUpdateID := 0
	wasLeader := false

	for {
		if r.State() != raft.Leader {
			wasLeader = false
			time.Sleep(1 * time.Second)
			continue
		}

		if !wasLeader {
			wasLeader = true
			log.Println("🔄 Leader active: Flushing old Telegram messages...")
			lastUpdateID = flushUpdates(bot, lastUpdateID)
			log.Println("✅ Message Flush Complete. Ready.")

			fsm.mu.RLock()
			state := fsm.state
			fsm.mu.RUnlock()

			if state.AuthorizedUserID == 0 {
				if state.InitialArmCode == "" {
					state.InitialArmCode = fmt.Sprintf("%06d", cryptoRandIntn(1000000))
					proposeState(r, state)
					log.Printf("🔑 NEW ONE-TIME ARM CODE GENERATED: %s  (only required on first-ever arm)", state.InitialArmCode)
				} else {
					log.Printf("🔑 EXISTING ONE-TIME ARM CODE RESTORED: %s  (only required on first-ever arm)", state.InitialArmCode)
				}
			}
		}

		fsm.mu.RLock()
		state := fsm.state
		fsm.mu.RUnlock()

		if state.Triggered {
			time.Sleep(1 * time.Second)
			continue
		}

		chatID := effectiveChatID(conf, state)

		// --- SNOOZE WAKEUP ---
		if state.Snoozed && !state.Disabled && time.Now().Unix() > state.WakeupTime {
			q := cryptoRandIntn(200) + 50

			state.Snoozed = false
			state.Armed = true
			state.Deadline = time.Now().Add(timerDuration).Unix()
			state.Answer = q + conf.AnswerOffset
			state.OriginalDuration = int64(timerDuration)
			state.WarningLevel = 0
			appendHistory(&state, "Armed (snooze wakeup)", conf.NodeID, 0)

			if proposeState(r, state) {
				log.Printf("⏰ WAKEUP: Cycle started. Query: %d. Duration: %s", q, timerDuration)
				msg := newMsg(chatID, fmt.Sprintf(
					"⏰ *Timer initiated via* `%s`\nTimer duration: %s\nQuery: `%d`",
					conf.NodeID, formatDuration(timerDuration), q))
				msg.ParseMode = "Markdown"
				sendMsg(bot, conf, msg)
				lastUpdateID = flushUpdates(bot, lastUpdateID)
			}
			continue
		}

		// --- WARNING LOGIC ---
		if state.Armed && !state.Snoozed {
			timeLeft := time.Until(time.Unix(state.Deadline, 0))
			totalDur := time.Duration(state.OriginalDuration)
			if totalDur == 0 {
				totalDur = timerDuration
			}
			ratio := float64(timeLeft) / float64(totalDur)

			newLevel := 0
			msgText := ""
			smtpSubject := ""

			if ratio <= 0.10 && state.WarningLevel < 3 {
				newLevel = 3
				msgText = fmt.Sprintf("⚠️ *CRITICAL: 10%% Time Remaining* (`%s`)", timeLeft.Round(time.Second))
				smtpSubject = "Deadman Switch — CRITICAL: 10% Time Remaining"
				log.Printf("⚠️ WARNING DISPATCH: 10%% remaining (%s)", timeLeft.Round(time.Second))
			} else if ratio <= 0.25 && state.WarningLevel < 2 {
				newLevel = 2
				msgText = fmt.Sprintf("⚠️ *WARNING: 25%% Time Remaining* (`%s`)", timeLeft.Round(time.Second))
				smtpSubject = "Deadman Switch — WARNING: 25% Time Remaining"
				log.Printf("⚠️ WARNING DISPATCH: 25%% remaining (%s)", timeLeft.Round(time.Second))
			} else if ratio <= 0.50 && state.WarningLevel < 1 {
				newLevel = 1
				msgText = fmt.Sprintf("⚠️ *NOTICE: 50%% Time Remaining* (`%s`)", timeLeft.Round(time.Second))
				smtpSubject = "Deadman Switch — NOTICE: 50% Time Remaining"
				log.Printf("⚠️ WARNING DISPATCH: 50%% remaining (%s)", timeLeft.Round(time.Second))
			}

			if newLevel > 0 {
				fsm.mu.RLock()
				state = fsm.state
				fsm.mu.RUnlock()
				state.WarningLevel = newLevel
				appendHistory(&state, smtpSubject, conf.NodeID, 0)
				if proposeState(r, state) {
					sendWarning(bot, conf, chatID, smtpSubject, msgText)
				}
			}
		}

		// --- DEADLINE CHECK ---
		if state.Armed && !state.Snoozed && time.Now().Unix() > state.Deadline {
			log.Printf("💀 DEADLINE EXPIRED. INITIATING PAYLOAD EXECUTION.")
			appendHistory(&state, "Payload triggered", conf.NodeID, 0)
			sendWarning(bot, conf, chatID,
				"Deadman Switch — Timer Expired",
				fmt.Sprintf("💀 *Timer expired* — Executing payload via host `%s`", conf.NodeID))

			cmd := exec.Command(conf.PayloadPath)
			output, err := cmd.CombinedOutput()
			if err != nil {
				log.Printf("❌ PAYLOAD FAILED: %v", err)
			} else {
				log.Printf("✅ PAYLOAD SUCCESS")
			}
			log.Printf("📝 OUTPUT START\n%s\n📝 OUTPUT END", string(output))

			state.Triggered = true
			proposeState(r, state)
			continue
		}

		// --- POLL FOR MESSAGES ---
		u := tgbotapi.NewUpdate(lastUpdateID + 1)
		u.Timeout = 30
		updates, err := bot.GetUpdates(u)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		for _, update := range updates {
			lastUpdateID = update.UpdateID
			if update.Message == nil {
				continue
			}

			cmd, args := parseCommand(update.Message.Text)

			if state.AuthorizedUserID == 0 {
				if cmd == "/init_arm" {
					targetChat := conf.TelegramChatID
					if targetChat == 0 {
						targetChat = update.Message.Chat.ID
					}

					if args == state.InitialArmCode {
						q := cryptoRandIntn(200) + 50
						state.AuthorizedUserID = update.Message.From.ID
						state.ChatID = targetChat
						state.Armed = true
						state.Deadline = time.Now().Add(timerDuration).Unix()
						state.Answer = q + conf.AnswerOffset
						state.Snoozed = false
						state.OriginalDuration = int64(timerDuration)
						state.WarningLevel = 0
						appendHistory(&state, "Armed (initial)", conf.NodeID, update.Message.From.ID)

						if proposeState(r, state) {
							log.Printf("🛡️ FIRST ARM: UserID %d registered as authorized user. Duration: %s",
								update.Message.From.ID, timerDuration)
							msg := newMsg(targetChat, fmt.Sprintf(
								"🛡️ *Authorized user registered.*\n⏰ *Timer initiated via* `%s`\nTimer duration: %s\nQuery: `%d`",
								conf.NodeID, formatDuration(timerDuration), q))
							msg.ParseMode = "Markdown"
							sendMsg(bot, conf, msg)

							// Send a test email async to verify SMTP settings on first arm
							go func() {
								if conf.SMTPHost == "" || conf.SMTPRecipient == "" {
									smtpNote := newMsg(targetChat, "⚠️ *SMTP not configured.* Email warnings will not be sent when the timer is running low.")
									smtpNote.ParseMode = "Markdown"
									sendMsg(bot, conf, smtpNote)
								} else {
									smtpErr := sendSMTP(conf, "Deadman Switch — SMTP Test", fmt.Sprintf(
										"This is a test email sent when the deadman switch was armed for the first time on node %s.\n\nIf you received this, your SMTP settings are correctly configured and timer warning emails will be delivered to this address.\n\nTimer duration: %s",
										conf.NodeID, formatDuration(timerDuration)))
									var smtpNote tgbotapi.MessageConfig
									if smtpErr != nil {
										log.Printf("⚠️ SMTP test failed: %v", smtpErr)
										smtpNote = newMsg(targetChat, fmt.Sprintf(
											"⚠️ *SMTP test failed* — timer warning emails will not be delivered.\nError: `%s`\n\nCheck your SMTP settings in `config.json`.", smtpErr.Error()))
									} else {
										smtpNote = newMsg(targetChat, fmt.Sprintf(
											"📧 *SMTP test email sent* to `%s`. If received, email warnings are working correctly.\n_Note: delivery cannot be confirmed from this end._", conf.SMTPRecipient))
									}
									smtpNote.ParseMode = "Markdown"
									sendMsg(bot, conf, smtpNote)
								}
							}()

							fsm.mu.RLock()
							state = fsm.state
							fsm.mu.RUnlock()
						}
					} else if args == "" {
						msg := newMsg(targetChat, fmt.Sprintf(
							"ℹ️ *System is unauthorized.*\nTo arm the system for the first time, check the logs on the Leader node (`%s`) for the one-time arm code.\n\nRun:\n`journalctl -u deadman.service`\n\nThen reply with:\n`/init_arm <code>`", conf.NodeID))
						msg.ParseMode = "Markdown"
						sendMsg(bot, conf, msg)
					}
				}
				continue
			}

			if !isAuthorized(update.Message.From.ID, state.AuthorizedUserID, conf.AuthorizedUserIDs) {
				continue
			}
			if cmd == "" || !strings.HasPrefix(cmd, "/") {
				continue
			}

			log.Printf("📩 CMD RECEIVED: %s %s", cmd, args)

			switch cmd {
			case "/init_arm":
				if state.Armed {
					msg := newMsg(chatID, "ℹ️ System is already armed.")
					sendMsg(bot, conf, msg)
					continue
				}
				q := cryptoRandIntn(200) + 50

				state.Armed = true
				state.Deadline = time.Now().Add(timerDuration).Unix()
				state.Answer = q + conf.AnswerOffset
				state.Snoozed = false
				state.OriginalDuration = int64(timerDuration)
				state.WarningLevel = 0
				appendHistory(&state, "Armed", conf.NodeID, update.Message.From.ID)

				if proposeState(r, state) {
					log.Printf("🛡️ SYSTEM ARMED. Duration: %s", timerDuration)
					msg := newMsg(chatID, fmt.Sprintf(
						"⏰ *Timer initiated via* `%s`\nTimer duration: %s\nQuery: `%d`",
						conf.NodeID, formatDuration(timerDuration), q))
					msg.ParseMode = "Markdown"
					sendMsg(bot, conf, msg)
				}

			case "/arm":
				if state.Armed && !state.Snoozed {
					msg := newMsg(chatID, "ℹ️ System is already armed and counting down.")
					sendMsg(bot, conf, msg)
					continue
				}
				q := cryptoRandIntn(200) + 50
				state.Armed = true
				state.Disabled = false
				state.Deadline = time.Now().Add(timerDuration).Unix()
				state.Answer = q + conf.AnswerOffset
				state.Snoozed = false
				state.OriginalDuration = int64(timerDuration)
				state.WarningLevel = 0
				appendHistory(&state, "Armed (manual)", conf.NodeID, update.Message.From.ID)

				if proposeState(r, state) {
					log.Printf("🛡️ SYSTEM MANUALLY ARMED.")
					msg := newMsg(chatID, fmt.Sprintf(
						"⏰ *Manual re-arm initiated via* `%s`\nTimer duration: %s\nQuery: `%d`",
						conf.NodeID, formatDuration(timerDuration), q))
					msg.ParseMode = "Markdown"
					sendMsg(bot, conf, msg)
				}

			case "/test":
				if state.TestInProgress {
					msg := newMsg(chatID, "⏳ A test is already in progress.")
					sendMsg(bot, conf, msg)
					continue
				}
				log.Printf("🧪 MANUAL TEST: Triggered by UserID: %d", update.Message.From.ID)
				msg := newMsg(chatID, "🧪 *Manual test starting across all nodes…*")
				msg.ParseMode = "Markdown"
				sendMsg(bot, conf, msg)
				initiateTest(r, fsm, conf, "manual")

			case "/status":
				var statusLines []string
				if state.TestInProgress {
					completed := state.TestCurrentIdx
					total := len(state.TestNodes)
					current := "—"
					if completed < total {
						current = state.TestNodes[completed]
					}
					statusLines = append(statusLines, fmt.Sprintf(
						"🧪 *Test in progress:* %d/%d nodes complete, running on `%s`",
						completed, total, current))
				}

				if state.Snoozed {
					timeToArm := time.Until(time.Unix(state.WakeupTime, 0))
					statusLines = append(statusLines, fmt.Sprintf(
						"🟡 *State:* Snoozed — rearming in `%s`", timeToArm.Round(time.Second)))
				} else if state.Disabled {
					statusLines = append(statusLines, "🔴 *State:* Disabled — will not rearm until `/arm` is sent")
				} else if state.Armed {
					timeLeft := time.Until(time.Unix(state.Deadline, 0))
					totalDur := time.Duration(state.OriginalDuration)
					if totalDur == 0 {
						totalDur = timerDuration
					}
					pct := 0.0
					if totalDur > 0 {
						pct = float64(timeLeft) / float64(totalDur) * 100
					}
					if pct < 0 {
						pct = 0
					}

					// Reverse the offset to show the original query prompt
					originalQuery := state.Answer - conf.AnswerOffset

					statusLines = append(statusLines, fmt.Sprintf(
						"🟢 *State:* Armed — `%s` remaining (%.1f%%)\n❓ *Query:* `%d`",
						timeLeft.Round(time.Second), pct, originalQuery))
				} else if state.Triggered {
					statusLines = append(statusLines, "💀 *State:* Triggered — payload has been executed")
				} else {
					statusLines = append(statusLines, "⚪ *State:* Unarmed")
				}

				peerLines := fmt.Sprintf("👑 *This node:* `%s` ✅", conf.NodeID)
				for _, peer := range conf.Peers {
					conn, err := tls.DialWithDialer(
						&net.Dialer{Timeout: 2 * time.Second}, "tcp", peer, tlsConfig)
					if err != nil {
						peerLines += fmt.Sprintf("\n🖥️ *Peer:* `%s` ❌", peer)
					} else {
						conn.Close()
						peerLines += fmt.Sprintf("\n🖥️ *Peer:* `%s` ✅", peer)
					}
				}
				statusLines = append(statusLines, peerLines)

				reply := newMsg(chatID, strings.Join(statusLines, "\n"))
				reply.ParseMode = "Markdown"
				sendMsg(bot, conf, reply)

			case "/disarm":
				if !state.Armed || state.Snoozed {
					msg := newMsg(chatID, "ℹ️ System is not currently armed or is already snoozed.")
					sendMsg(bot, conf, msg)
					continue
				}
				if time.Now().Unix() < state.NextAttemptUnix {
					wait := time.Until(time.Unix(state.NextAttemptUnix, 0)).Round(time.Second)
					msg := newMsg(chatID, fmt.Sprintf("⛔ Locked out. Try again in %s.", wait))
					sendMsg(bot, conf, msg)
					log.Printf("⛔ IGNORED: User is locked out for %s.", wait)
					continue
				}

				expected := fmt.Sprintf("%d", state.Answer)
				if args == expected {
					log.Printf("✅ DISARM SUCCESS: Resetting Timer.")
					state.Snoozed = true
					state.WakeupTime = state.Deadline
					state.FailureCount = 0
					state.WarningLevel = 0
					appendHistory(&state, "Disarmed", conf.NodeID, update.Message.From.ID)

					if proposeState(r, state) {
						delMsg := tgbotapi.NewDeleteMessage(update.Message.Chat.ID, update.Message.MessageID)
						rearmTime := time.Unix(state.WakeupTime, 0).Format("Mon 02 Jan 2006 at 15:04:05 MST")
						responseText := fmt.Sprintf("✅ Timer disarmed, user response redacted. Will automatically re-arm on %s.", rearmTime)
						if _, err := bot.Request(delMsg); err != nil {
							log.Printf("⚠️ Failed to delete disarm message (check bot admin permissions): %v", err)
							responseText = fmt.Sprintf("✅ Timer disarmed. Will automatically re-arm on %s.\n⚠️ *Warning:* Could not redact user response! Please grant the bot 'Delete messages' admin permissions.", rearmTime)
						}
						msg := newMsg(chatID, responseText)
						msg.ParseMode = "Markdown"
						sendMsg(bot, conf, msg)
						lastUpdateID = flushUpdates(bot, lastUpdateID)
					}
				} else {
					state.FailureCount++
					shift := state.FailureCount - 1
					if shift > 9 {
						shift = 9
					}
					delay := 5 * (1 << shift)
					state.NextAttemptUnix = time.Now().Add(time.Duration(delay) * time.Second).Unix()
					log.Printf("❌ DISARM INVALID: Expected \"%s\", Got \"%s\". Cooldown: %ds", expected, args, delay)
					proposeState(r, state)
					msg := newMsg(chatID, fmt.Sprintf("❌ Incorrect code — Next attempt in: %ds", delay))
					sendMsg(bot, conf, msg)
				}

			case "/disable":
				if conf.DisablePassphrase == "" {
					msg := newMsg(chatID, "⛔ The `/disable` command is not configured on this node.")
					msg.ParseMode = "Markdown"
					sendMsg(bot, conf, msg)
					continue
				}
				if time.Now().Unix() < state.DisableNextAttemptUnix {
					wait := time.Until(time.Unix(state.DisableNextAttemptUnix, 0)).Round(time.Second)
					msg := newMsg(chatID, fmt.Sprintf("⛔ Locked out. Try again in %s.", wait))
					sendMsg(bot, conf, msg)
					log.Printf("⛔ DISABLE LOCKED OUT: %s remaining.", wait)
					continue
				}
				if subtle.ConstantTimeCompare([]byte(args), []byte(conf.DisablePassphrase)) != 1 {
					state.DisableFailureCount++
					shift := state.DisableFailureCount - 1
					if shift > 9 {
						shift = 9
					}
					delay := 5 * (1 << shift)
					state.DisableNextAttemptUnix = time.Now().Add(time.Duration(delay) * time.Second).Unix()
					log.Printf("❌ DISABLE REJECTED: Incorrect passphrase from UserID %d. Cooldown: %ds", update.Message.From.ID, delay)
					proposeState(r, state)
					// Attempt to redact the failed attempt — passphrase guess should not linger in chat
					bot.Request(tgbotapi.NewDeleteMessage(update.Message.Chat.ID, update.Message.MessageID))
					msg := newMsg(chatID, fmt.Sprintf("❌ Incorrect passphrase — Next attempt in: %ds", delay))
					sendMsg(bot, conf, msg)
					continue
				}

				wasArmed := state.Armed || state.Snoozed
				state.Armed = false
				state.Snoozed = false
				state.Disabled = true
				state.WarningLevel = 0
				state.FailureCount = 0
				state.NextAttemptUnix = 0
				state.DisableFailureCount = 0
				state.DisableNextAttemptUnix = 0
				appendHistory(&state, "Disabled", conf.NodeID, update.Message.From.ID)

				if proposeState(r, state) {
					delMsg := tgbotapi.NewDeleteMessage(update.Message.Chat.ID, update.Message.MessageID)
					responseText := "🔴 *System disabled.* The switch will not rearm until an `/arm` command is sent."
					if wasArmed {
						responseText = "🔴 *System disarmed and disabled.* The switch will not rearm until an `/arm` command is sent."
					}
					if _, err := bot.Request(delMsg); err != nil {
						log.Printf("⚠️ Failed to delete /disable message: %v", err)
						responseText += "\n⚠️ *Warning:* Could not redact your message! Please grant the bot 'Delete messages' admin permissions."
					}
					log.Printf("🔴 SYSTEM DISABLED by UserID %d (was armed: %v)", update.Message.From.ID, wasArmed)
					msg := newMsg(chatID, responseText)
					msg.ParseMode = "Markdown"
					sendMsg(bot, conf, msg)
					lastUpdateID = flushUpdates(bot, lastUpdateID)
				}

			case "/extend":
				if !state.Armed || state.Snoozed {
					msg := newMsg(chatID, "ℹ️ System is not currently armed.")
					sendMsg(bot, conf, msg)
					continue
				}
				if time.Now().Unix() < state.NextAttemptUnix {
					wait := time.Until(time.Unix(state.NextAttemptUnix, 0)).Round(time.Second)
					msg := newMsg(chatID, fmt.Sprintf("⛔ Locked out. Try again in %s.", wait))
					sendMsg(bot, conf, msg)
					continue
				}
				extParts := strings.SplitN(args, " ", 2)
				if len(extParts) != 2 {
					msg := newMsg(chatID, "Usage: `/extend <duration> <code>`\nExample: `/extend 2d 153`")
					msg.ParseMode = "Markdown"
					sendMsg(bot, conf, msg)
					continue
				}
				extDur, extErr := parseDuration(extParts[0])
				if extErr != nil || extDur <= 0 {
					msg := newMsg(chatID, "❌ Invalid duration. Use formats like `2d`, `12h`, `30m`.")
					msg.ParseMode = "Markdown"
					sendMsg(bot, conf, msg)
					continue
				}
				extExpected := fmt.Sprintf("%d", state.Answer)
				if extParts[1] != extExpected {
					state.FailureCount++
					shift := state.FailureCount - 1
					if shift > 9 {
						shift = 9
					}
					delay := 5 * (1 << shift)
					state.NextAttemptUnix = time.Now().Add(time.Duration(delay) * time.Second).Unix()
					log.Printf("❌ EXTEND INVALID: Expected \"%s\", Got \"%s\". Cooldown: %ds", extExpected, extParts[1], delay)
					proposeState(r, state)
					bot.Request(tgbotapi.NewDeleteMessage(update.Message.Chat.ID, update.Message.MessageID))
					msg := newMsg(chatID, fmt.Sprintf("❌ Incorrect code — Next attempt in: %ds", delay))
					sendMsg(bot, conf, msg)
					continue
				}
				q := cryptoRandIntn(200) + 50
				state.Deadline += int64(extDur.Seconds())
				state.OriginalDuration += int64(extDur)
				state.Answer = q + conf.AnswerOffset
				state.WarningLevel = 0
				state.FailureCount = 0
				state.NextAttemptUnix = 0
				appendHistory(&state, fmt.Sprintf("Extended by %s", formatDuration(extDur)), conf.NodeID, update.Message.From.ID)

				if proposeState(r, state) {
					bot.Request(tgbotapi.NewDeleteMessage(update.Message.Chat.ID, update.Message.MessageID))
					newDeadline := time.Unix(state.Deadline, 0).Format("Mon 02 Jan 2006 at 15:04:05 MST")
					log.Printf("⏱️ TIMER EXTENDED by %s by UserID %d. New deadline: %s",
						formatDuration(extDur), update.Message.From.ID, newDeadline)
					msg := newMsg(chatID, fmt.Sprintf(
						"⏱️ *Timer extended* by `%s`.\nNew deadline: %s\nQuery: `%d`",
						formatDuration(extDur), newDeadline, q))
					msg.ParseMode = "Markdown"
					sendMsg(bot, conf, msg)
				}

			case "/history":
				if len(state.EventHistory) == 0 {
					msg := newMsg(chatID, "📋 No events recorded yet.")
					sendMsg(bot, conf, msg)
					continue
				}
				lines := []string{"📋 *Recent Events:*"}
				for i := len(state.EventHistory) - 1; i >= 0; i-- {
					e := state.EventHistory[i]
					t := time.Unix(e.Time, 0).Format("02 Jan 06 15:04 MST")
					line := fmt.Sprintf("`%s` — %s", t, e.Event)
					if e.NodeID != "" {
						line += fmt.Sprintf(" _(%s)_", e.NodeID)
					}
					lines = append(lines, line)
				}
				histMsg := newMsg(chatID, strings.Join(lines, "\n"))
				histMsg.ParseMode = "Markdown"
				sendMsg(bot, conf, histMsg)
			}
		}
	}
}

// --- Utilities ---

func flushUpdates(bot *tgbotapi.BotAPI, currentID int) int {
	u := tgbotapi.NewUpdate(currentID + 1)
	u.Timeout = 0
	updates, _ := bot.GetUpdates(u)
	maxID := currentID
	for _, up := range updates {
		if up.UpdateID >= maxID {
			maxID = up.UpdateID
		}
	}
	return maxID
}

func proposeState(r *raft.Raft, s DeadmanState) bool {
	b, _ := json.Marshal(s)
	f := r.Apply(b, 5*time.Second)
	if err := f.Error(); err != nil {
		log.Printf("❌ Raft Apply Error: %v", err)
		return false
	}
	return true
}

func formatDuration(d time.Duration) string {
	days := int64(d.Hours()) / 24
	if days > 0 {
		hours := int64(d.Hours()) % 24
		minutes := int64(d.Minutes()) % 60
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	}
	return d.String()
}

// --- Networking & TLS ---

func setupTLS(c Config) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	caPool, err := x509.SystemCertPool()
	if err != nil {
		caPool = x509.NewCertPool()
	}
	if c.CACert != "" {
		caPEM, err := os.ReadFile(c.CACert)
		if err != nil {
			return nil, fmt.Errorf("ca_cert: %w", err)
		}
		if !caPool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("ca_cert: no valid certificates found in %s", c.CACert)
		}
	}
	clientAuth := tls.RequireAnyClientCert
	if c.CACert != "" {
		clientAuth = tls.RequireAndVerifyClientCert
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		RootCAs:      caPool,
		ClientAuth:   clientAuth,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func waitForPeers(c Config, t *tls.Config) {
	log.Println("BOOTSTRAP: Waiting for all peers to come online...")
	for {
		ready := 0
		for _, peer := range c.Peers {
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", peer, t)
			if err == nil {
				conn.Close()
				ready++
			}
		}
		if ready == len(c.Peers) {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

type peekedConn struct {
	net.Conn
	peeked     byte
	peekedRead bool
}

func (c *peekedConn) Read(b []byte) (n int, err error) {
	if !c.peekedRead {
		if len(b) == 0 {
			return 0, nil
		}
		b[0] = c.peeked
		c.peekedRead = true
		if len(b) > 1 {
			n, err = c.Conn.Read(b[1:])
			return n + 1, err
		}
		return 1, nil
	}
	return c.Conn.Read(b)
}

type TLSStreamLayer struct {
	ln       net.Listener
	cfg      *tls.Config
	raftConn chan net.Conn
	rpcConn  chan net.Conn
	errCh    chan error
	closeCh  chan struct{}
	closeMu  sync.Mutex
	closed   bool
	addrStr  string
}

func newTLSStreamLayer(addr string, advertise string, t *tls.Config) (*TLSStreamLayer, error) {
	ln, err := tls.Listen("tcp", addr, t)
	if err != nil {
		return nil, err
	}
	sl := &TLSStreamLayer{
		ln:       ln,
		cfg:      t,
		addrStr:  advertise,
		raftConn: make(chan net.Conn),
		rpcConn:  make(chan net.Conn),
		errCh:    make(chan error),
		closeCh:  make(chan struct{}),
	}
	go sl.listenLoop()
	return sl, nil
}

func (t *TLSStreamLayer) listenLoop() {
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			select {
			case <-t.closeCh:
				return
			case t.errCh <- err:
			}
			return
		}
		go func(c net.Conn) {
			c.SetReadDeadline(time.Now().Add(3 * time.Second))
			var b [1]byte
			if _, err := io.ReadFull(c, b[:]); err != nil {
				c.Close()
				return
			}
			c.SetReadDeadline(time.Time{})

			pc := &peekedConn{Conn: c, peeked: b[0]}
			if b[0] == 0xFF {
				select {
				case t.rpcConn <- pc:
				case <-t.closeCh:
					c.Close()
				}
			} else {
				select {
				case t.raftConn <- pc:
				case <-t.closeCh:
					c.Close()
				}
			}
		}(conn)
	}
}

func (t *TLSStreamLayer) Accept() (net.Conn, error) {
	select {
	case c := <-t.raftConn:
		return c, nil
	case err := <-t.errCh:
		return nil, err
	case <-t.closeCh:
		return nil, io.EOF
	}
}

func (t *TLSStreamLayer) Close() error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	if !t.closed {
		t.closed = true
		close(t.closeCh)
		return t.ln.Close()
	}
	return nil
}

type customAddr struct{ addr string }

func (a customAddr) Network() string { return "tcp" }
func (a customAddr) String() string  { return a.addr }

func (t *TLSStreamLayer) Addr() net.Addr { return customAddr{addr: t.addrStr} }

func (t *TLSStreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp4", string(addr), t.cfg)
}
