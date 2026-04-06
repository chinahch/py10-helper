package main

import (
    "bufio"
    "bytes"
    "encoding/base64"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "time"
)

type any = interface{}

func main() {
    if len(os.Args) < 2 {
        usage()
        os.Exit(1)
    }
    cmd := os.Args[1]
    var err error
    switch cmd {
    case "set-script-var":
        err = cmdSetScriptVar(os.Args[2:])
    case "url-query-param":
        err = cmdURLQueryParam(os.Args[2:])
    case "url-fragment":
        err = cmdURLFragment(os.Args[2:])
    case "url-decode":
        err = cmdURLDecode(os.Args[2:])
    case "pad-text":
        err = cmdPadText(os.Args[2:])
    case "pick-free-port":
        err = cmdPickFreePort()
    case "vmess-rewrite-host":
        err = cmdVmessRewriteHost(os.Args[2:])
    case "link-to-outbound-json", "parse-node-link":
        err = cmdLinkToOutbound(os.Args[2:])
    case "wait-port":
        err = cmdWaitPort(os.Args[2:])
    case "vless-build-cfg", "realtest-vless-cfg":
        err = cmdVlessBuildCfg(os.Args[1:])
    case "sort-ingress":
        err = cmdSortIngress(os.Args[2:])
    case "parse-ss-plugin-stream":
        err = cmdParseSSPluginStream(os.Args[2:])
    case "get-original-path":
        err = cmdGetOriginalPath(os.Args[2:])
    case "ensure-sing-origin-template":
        err = cmdEnsureSingOriginTemplate(os.Args[2:])
    case "sync-route-only":
        err = cmdSyncRouteOnly(os.Args[2:])
    case "validate-config-json":
        err = cmdValidateConfigJSON(os.Args[2:])
    case "merge-nodes-config":
        err = cmdMergeNodesConfig(os.Args[2:])
    case "verify-added-nodes":
        err = cmdVerifyAddedNodes(os.Args[2:])
    case "send-telegram":
        err = cmdSendTelegram(os.Args[2:])
    case "list-nodes":
        err = cmdListNodes(os.Args[2:])
    case "delete-nodes":
        err = cmdDeleteNodes(os.Args[2:])
    case "list-proxy-outbounds":
        err = cmdListProxyOutbounds(os.Args[2:])
    case "jq-compat":
        err = cmdJQCompat(os.Args[2:])
    default:
        usage()
        os.Exit(1)
    }
    if err != nil {
        fmt.Fprintln(os.Stderr, err.Error())
        os.Exit(1)
    }
}

func usage() {
    fmt.Println("usage: py10-helper <command> [args...]")
}

func cmdSetScriptVar(args []string) error {
    if len(args) != 3 {
        return errors.New("set-script-var <script> <VAR> <value>")
    }
    p, name, val := args[0], args[1], args[2]
    b, err := os.ReadFile(p)
    if err != nil {
        return err
    }
    s := string(b)
    re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `="[^"]*"`)
    replaced := re.ReplaceAllString(s, fmt.Sprintf(`%s="%s"`, name, escapeDouble(val)))
    if replaced == s {
        return fmt.Errorf("variable not found: %s", name)
    }
    return os.WriteFile(p, []byte(replaced), 0755)
}

func escapeDouble(s string) string {
    s = strings.ReplaceAll(s, `\`, `\\`)
    s = strings.ReplaceAll(s, `"`, `\"`)
    return s
}

func cmdURLQueryParam(args []string) error {
    if len(args) != 2 { return errors.New("url-query-param <url> <key>") }
    u, err := url.Parse(args[0]); if err != nil { return err }
    fmt.Print(u.Query().Get(args[1]))
    return nil
}
func cmdURLFragment(args []string) error {
    if len(args) != 1 { return errors.New("url-fragment <url>") }
    u, err := url.Parse(args[0]); if err != nil { return err }
    fmt.Print(u.Fragment)
    return nil
}
func cmdURLDecode(args []string) error {
    if len(args) != 1 { return errors.New("url-decode <value>") }
    v, err := url.QueryUnescape(args[0]); if err != nil { return err }
    fmt.Print(v)
    return nil
}
func cmdPadText(args []string) error {
    if len(args) != 2 { return errors.New("pad-text <text> <width>") }
    w, err := strconv.Atoi(args[1]); if err != nil { return err }
    txt := args[0]
    rw := 0
    for _, r := range txt {
        if r > 127 { rw += 2 } else { rw += 1 }
    }
    if rw >= w { fmt.Print(txt); return nil }
    fmt.Print(txt + strings.Repeat(" ", w-rw))
    return nil
}
func cmdPickFreePort() error {
    l, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil { return err }
    defer l.Close()
    fmt.Print(l.Addr().(*net.TCPAddr).Port)
    return nil
}
func cmdWaitPort(args []string) error {
    if len(args) != 1 { return errors.New("wait-port <port>") }
    port := args[0]
    deadline := time.Now().Add(8 * time.Second)
    for time.Now().Before(deadline) {
        c, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 300*time.Millisecond)
        if err == nil {
            c.Close(); return nil
        }
        time.Sleep(100 * time.Millisecond)
    }
    return errors.New("port not ready")
}

func cmdVmessRewriteHost(args []string) error {
    if len(args) != 2 { return errors.New("vmess-rewrite-host <link> <new-host>") }
    payload := strings.TrimPrefix(args[0], "vmess://")
    jb, err := b64DecodeAuto(payload)
    if err != nil { return err }
    var obj map[string]any
    if err := json.Unmarshal(jb, &obj); err != nil { return err }
    obj["add"] = args[1]
    out, _ := json.Marshal(obj)
    fmt.Print("vmess://" + base64.StdEncoding.EncodeToString(out))
    return nil
}

func cmdLinkToOutbound(args []string) error {
    var link string
    if len(args) > 0 { link = args[0] } else {
        b, _ := io.ReadAll(os.Stdin)
        link = strings.TrimSpace(string(b))
    }
    if link == "" { return errors.New("empty link") }
    obj, err := parseNodeLink(link)
    if err != nil { return err }
    enc := json.NewEncoder(os.Stdout)
    enc.SetEscapeHTML(false)
    return enc.Encode(obj)
}

func parseNodeLink(link string) (map[string]any, error) {
    switch {
    case strings.HasPrefix(link, "vless://"):
        return parseVLESSOutbound(link)
    case strings.HasPrefix(link, "vmess://"):
        return parseVMessOutbound(link)
    case strings.HasPrefix(link, "trojan://"):
        return parseTrojanOutbound(link)
    case strings.HasPrefix(link, "ss://"):
        return parseSSOutbound(link)
    case strings.HasPrefix(link, "socks://") || strings.HasPrefix(link, "socks5://"):
        return parseSocksOutbound(link)
    default:
        return nil, fmt.Errorf("unsupported link: %s", link)
    }
}

func parseVLESSOutbound(link string) (map[string]any, error) {
    u, err := url.Parse(link)
    if err != nil { return nil, err }
    uuid := u.User.Username()
    host := u.Hostname()
    port, _ := strconv.Atoi(defaultIfEmpty(u.Port(), "443"))
    q := u.Query()
    security := defaultIfEmpty(q.Get("security"), "none")
    network := defaultIfEmpty(q.Get("type"), defaultIfEmpty(q.Get("net"), "tcp"))
    path, _ := url.QueryUnescape(defaultIfEmpty(q.Get("path"), "/"))
    wsHost, _ := url.QueryUnescape(q.Get("host"))
    sni, _ := url.QueryUnescape(q.Get("sni"))
    fp := q.Get("fp")
    pbk := q.Get("pbk")
    sid := q.Get("sid")
    flow := q.Get("flow")
    serverName := firstNonEmpty(sni, wsHost, host)

    vnextUser := map[string]any{"id": uuid, "encryption": "none"}
    if flow != "" { vnextUser["flow"] = flow }
    outbound := map[string]any{
        "protocol": "vless",
        "settings": map[string]any{
            "vnext": []any{map[string]any{
                "address": host,
                "port": port,
                "users": []any{vnextUser},
            }},
        },
    }
    stream := map[string]any{"network": network, "security": security}
    if network == "ws" {
        stream["wsSettings"] = map[string]any{
            "path": path,
            "headers": map[string]any{"Host": wsHost},
        }
    }
    if security == "tls" {
        tls := map[string]any{"serverName": serverName}
        if fp != "" { tls["fingerprint"] = fp }
        stream["tlsSettings"] = tls
    }
    if security == "reality" {
        rs := map[string]any{"serverName": serverName}
        if fp != "" { rs["fingerprint"] = fp }
        if pbk != "" { rs["publicKey"] = pbk }
        if sid != "" { rs["shortId"] = sid }
        stream["realitySettings"] = rs
    }
    outbound["streamSettings"] = stream
    return outbound, nil
}

func parseVMessOutbound(link string) (map[string]any, error) {
    raw := strings.TrimPrefix(link, "vmess://")
    jb, err := b64DecodeAuto(raw)
    if err != nil { return nil, err }
    var o map[string]any
    if err := json.Unmarshal(jb, &o); err != nil { return nil, err }
    add := str(o["add"])
    port, _ := strconv.Atoi(defaultIfEmpty(str(o["port"]), "443"))
    id := str(o["id"])
    netw := defaultIfEmpty(str(o["net"]), "tcp")
    path := defaultIfEmpty(str(o["path"]), "/")
    host := str(o["host"])
    sni := firstNonEmpty(str(o["sni"]), host, add)
    tlsMode := defaultIfEmpty(str(o["tls"]), "none")
    outbound := map[string]any{
        "protocol": "vmess",
        "settings": map[string]any{
            "vnext": []any{map[string]any{
                "address": add,
                "port": port,
                "users": []any{map[string]any{"id": id, "security": "auto"}},
            }},
        },
    }
    stream := map[string]any{"network": netw}
    if netw == "ws" {
        stream["wsSettings"] = map[string]any{"path": path, "headers": map[string]any{"Host": host}}
    }
    if tlsMode == "tls" {
        stream["security"] = "tls"
        stream["tlsSettings"] = map[string]any{"serverName": sni}
    }
    outbound["streamSettings"] = stream
    return outbound, nil
}

func parseTrojanOutbound(link string) (map[string]any, error) {
    u, err := url.Parse(link)
    if err != nil { return nil, err }
    pwd := u.User.Username()
    host := u.Hostname()
    port, _ := strconv.Atoi(defaultIfEmpty(u.Port(), "443"))
    sni := firstNonEmpty(u.Query().Get("sni"), host)
    return map[string]any{
        "protocol": "trojan",
        "settings": map[string]any{
            "servers": []any{map[string]any{"address": host, "port": port, "password": pwd}},
        },
        "streamSettings": map[string]any{
            "network": "tcp",
            "security": "tls",
            "tlsSettings": map[string]any{"serverName": sni},
        },
    }, nil
}

func parseSSOutbound(link string) (map[string]any, error) {
    raw := strings.TrimPrefix(link, "ss://")
    raw = strings.SplitN(raw, "#", 2)[0]
    var userInfo, hostPart string
    if strings.Contains(raw, "@") {
        parts := strings.SplitN(raw, "@", 2)
        userInfo, hostPart = parts[0], parts[1]
    } else {
        return nil, errors.New("invalid ss link")
    }
    dec, err := b64DecodeAuto(userInfo)
    if err != nil { return nil, err }
    creds := strings.SplitN(string(dec), ":", 2)
    if len(creds) != 2 { return nil, errors.New("invalid ss credentials") }
    method, password := creds[0], creds[1]
    hostPart = strings.SplitN(hostPart, "?", 2)[0]
    host, portStr, err := splitHostPortMaybeIPv6(hostPart)
    if err != nil { return nil, err }
    port, _ := strconv.Atoi(portStr)
    return map[string]any{
        "protocol": "shadowsocks",
        "settings": map[string]any{
            "servers": []any{map[string]any{"address": host, "port": port, "method": method, "password": password}},
        },
        "streamSettings": map[string]any{},
    }, nil
}

func parseSocksOutbound(link string) (map[string]any, error) {
    u, err := url.Parse(link)
    if err != nil { return nil, err }
    host := u.Hostname()
    port, _ := strconv.Atoi(defaultIfEmpty(u.Port(), "1080"))
    server := map[string]any{"address": host, "port": port}
    if u.User != nil {
        user := u.User.Username()
        pass, _ := u.User.Password()
        if user != "" {
            server["users"] = []any{map[string]any{"user": user, "pass": pass}}
        }
    }
    return map[string]any{
        "protocol": "socks",
        "settings": map[string]any{"servers": []any{server}},
        "streamSettings": map[string]any{},
    }, nil
}

func cmdParseSSPluginStream(args []string) error {
    if len(args) != 2 { return errors.New("parse-ss-plugin-stream <query> <host>") }
    vals, _ := url.ParseQuery(strings.ReplaceAll(args[0], ";", "&"))
    plugin := vals.Get("plugin")
    if plugin != "v2ray-plugin" { fmt.Print("{}"); return nil }
    opts := vals.Get("plugin-opts")
    optVals, _ := url.ParseQuery(strings.ReplaceAll(opts, ";", "&"))
    path := defaultIfEmpty(optVals.Get("path"), "/")
    tlsFlag := optVals.Get("tls")
    host := firstNonEmpty(optVals.Get("host"), args[1])
    obj := map[string]any{"network": "ws"}
    obj["wsSettings"] = map[string]any{"path": path, "headers": map[string]any{"Host": host}}
    if tlsFlag != "" {
        obj["security"] = "tls"
        obj["tlsSettings"] = map[string]any{"serverName": host}
    }
    return json.NewEncoder(os.Stdout).Encode(obj)
}

func cmdVlessBuildCfg(args []string) error {
    // supports: vless-build-cfg <uri> <cfg> <socksPort> <forceIPv4>
    // and: realtest-vless-cfg <uri> <socksPort>
    var uri, cfg string
    var port int
    forceIPv4 := true
    if args[0] == "vless-build-cfg" {
        if len(args) != 5 { return errors.New("vless-build-cfg <uri> <cfg> <socksPort> <forceIPv4>") }
        uri = args[1]; cfg = args[2]
        p, err := strconv.Atoi(args[3]); if err != nil { return err }; port = p
        forceIPv4 = args[4] == "1" || strings.EqualFold(args[4], "true")
    } else {
        if len(args) != 3 { return errors.New("realtest-vless-cfg <uri> <socksPort>") }
        uri = args[1]
        p, err := strconv.Atoi(args[2]); if err != nil { return err }; port = p
    }
    ob, err := parseVLESSOutbound(uri)
    if err != nil { return err }
    doc := map[string]any{
        "log": map[string]any{"loglevel": "warning"},
        "inbounds": []any{map[string]any{
            "listen": "127.0.0.1",
            "port": port,
            "protocol": "socks",
            "settings": map[string]any{"udp": false},
            "sniffing": map[string]any{"enabled": false},
        }},
        "outbounds": []any{withTag(ob, "proxy"), map[string]any{"protocol": "freedom", "tag": "direct"}},
        "routing": map[string]any{"domainStrategy": ternary(forceIPv4, "UseIPv4", "AsIs"), "rules": []any{map[string]any{"type": "field", "inboundTag": []any{"socks-in"}, "outboundTag": "proxy"}}},
    }
    // fix inbound tag
    inb := doc["inbounds"].([]any)[0].(map[string]any)
    inb["tag"] = "socks-in"
    data, _ := json.MarshalIndent(doc, "", "  ")
    if cfg != "" {
        return os.WriteFile(cfg, data, 0644)
    }
    _, err = os.Stdout.Write(data)
    return err
}

func withTag(m map[string]any, tag string) map[string]any { m["tag"] = tag; return m }

func cmdSortIngress(args []string) error {
    if len(args) != 1 { return errors.New("sort-ingress <file>") }
    p := args[0]
    b, err := os.ReadFile(p)
    if err != nil { return err }
    lines := strings.Split(string(b), "\n")
    var header []string
    var ingress []string
    var tail404 []string
    i := 0
    for i < len(lines) {
        line := lines[i]
        trim := strings.TrimSpace(line)
        if strings.HasPrefix(trim, "- hostname:") || strings.HasPrefix(trim, "- service:") {
            var block []string
            block = append(block, line)
            i++
            for i < len(lines) {
                t := strings.TrimSpace(lines[i])
                if strings.HasPrefix(t, "- hostname:") || strings.HasPrefix(t, "- service:") { break }
                block = append(block, lines[i]); i++
            }
            joined := strings.Join(block, "\n")
            if strings.Contains(joined, "http_status:404") { tail404 = append(tail404, block...) } else { ingress = append(ingress, block...) }
            continue
        }
        header = append(header, line)
        i++
    }
    out := append(header, ingress...)
    out = append(out, tail404...)
    return os.WriteFile(p, []byte(strings.Join(out, "\n")), 0644)
}

func cmdGetOriginalPath(args []string) error {
    if len(args) != 1 { return errors.New("get-original-path <config.json>") }
    var doc map[string]any
    if err := readJSONFile(args[0], &doc); err != nil { return err }
    cores, _ := doc["Cores"].([]any)
    for _, c := range cores {
        m, ok := c.(map[string]any); if !ok { continue }
        typ := str(m["Type"])
        op := str(m["OriginalPath"])
        if op != "" && (typ == "sspanel" || typ == "sing" || typ == "xray") {
            fmt.Print(op)
            return nil
        }
    }
    return nil
}

func cmdEnsureSingOriginTemplate(args []string) error {
    if len(args) != 1 { return errors.New("ensure-sing-origin-template <path>") }
    p := args[0]
    if _, err := os.Stat(p); err == nil { return nil }
    if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil { return err }
    tpl := map[string]any{
        "dns": map[string]any{"servers": []any{}},
        "outbounds": []any{map[string]any{"tag": "direct", "type": "direct"}, map[string]any{"tag": "block", "type": "block"}},
        "route": map[string]any{"rules": []any{}},
    }
    return writeJSONFile(p, tpl)
}

func cmdSyncRouteOnly(args []string) error {
    if len(args) != 2 { return errors.New("sync-route-only <route.json> <sing_origin.json>") }
    routeFile, originFile := args[0], args[1]
    var routeDoc map[string]any
    if err := readJSONFile(routeFile, &routeDoc); err != nil { return err }
    var originDoc map[string]any
    if err := readJSONFile(originFile, &originDoc); err != nil { return err }

    routeRules := []any{}
    if rules, ok := routeDoc["rules"].([]any); ok {
        routeRules = rules
    }

    // --- 核心修复：执行 Xray -> Sing-Box 全量语法翻译 ---
    translatedRules := []any{}
    for _, r := range routeRules {
        rm := jqMap(r)
        newR := make(map[string]any)
        for k, v := range rm {
            switch k {
            case "type": 
                continue // 彻底删除 "type": "field"
            case "outboundTag":
                newR["outbound"] = v 
            case "inboundTag":
                newR["inbound"] = v 
            case "port":
                // 核心翻译逻辑：将 "25,465,587" 转换为 [25, 465, 587]
                pVal := jqStr(v)
                if strings.Contains(pVal, ",") {
                    parts := strings.Split(pVal, ",")
                    intPorts := []int{}
                    for _, p := range parts {
                        p = strings.TrimSpace(p)
                        if iv, err := strconv.Atoi(p); err == nil {
                            intPorts = append(intPorts, iv)
                        }
                    }
                    newR["port"] = intPorts
                } else if iv, err := strconv.Atoi(pVal); err == nil {
                    newR["port"] = iv // 单个端口转成数字
                } else {
                    newR["port"] = pVal // 保持 "80:443" 这种范围字符串
                }
            case "ip":
                ips := jqArr(v)
                if len(ips) == 1 && jqStr(ips[0]) == "geoip:private" {
                    newR["ip_is_private"] = true
                } else {
                    newR[k] = v
                }
            default:
                newR[k] = v
            }
        }
        translatedRules = append(translatedRules, newR)
    }
    // ----------------------------------------------

    if originDoc["route"] == nil { originDoc["route"] = map[string]any{} }
    rmap := originDoc["route"].(map[string]any)
    rmap["rules"] = translatedRules
    return writeJSONFile(originFile, originDoc)
}

func cmdValidateConfigJSON(args []string) error {
    if len(args) != 1 { return errors.New("validate-config-json <config.json>") }
    var doc map[string]any
    if err := readJSONFile(args[0], &doc); err != nil { return err }
    if _, ok := doc["Nodes"].([]any); !ok { return errors.New("invalid config.json: Nodes missing or not array") }
    if _, ok := doc["Cores"].([]any); !ok { return errors.New("invalid config.json: Cores missing or not array") }
    fmt.Print("OK")
    return nil
}

func cmdMergeNodesConfig(args []string) error {
    if len(args) != 5 { return errors.New("merge-nodes-config <config.json> <nodes.json> <xray-block.json> <sing-block.json> <hy2-block.json>") }
    var doc map[string]any
    if err := readJSONFile(args[0], &doc); err != nil { return err }
    var newNodes []any
    if err := readJSONFile(args[1], &newNodes); err != nil { return err }
    var xb, sb, hb map[string]any
    if err := readJSONFile(args[2], &xb); err != nil { return err }
    if err := readJSONFile(args[3], &sb); err != nil { return err }
    if err := readJSONFile(args[4], &hb); err != nil { return err }

    nodes, _ := doc["Nodes"].([]any)
    nodes = append(nodes, newNodes...)
    sort.Slice(nodes, func(i, j int) bool {
        return nodeID(nodes[i]) < nodeID(nodes[j])
    })
    doc["Nodes"] = nodes

    cores, _ := doc["Cores"].([]any)
    hasXray, hasSing := false, false
    needXray, needSing, needHy2 := false, false, false
    for _, c := range cores {
        if m, ok := c.(map[string]any); ok {
            switch str(m["Type"]) {
            case "xray": hasXray = true
            case "sing", "hysteria2": hasSing = true
            }
        }
    }
    for _, n := range nodes {
        if m, ok := n.(map[string]any); ok {
            switch str(m["Core"]) {
            case "xray": needXray = true
            case "sing": needSing = true
            case "hysteria2": needHy2 = true
            }
        }
    }
    if needXray && !hasXray { cores = append(cores, xb) }
    if needSing && !hasSing { cores = append(cores, sb); hasSing = true }
    if needHy2 && !hasSing { cores = append(cores, hb) }
    doc["Cores"] = cores

    enc := json.NewEncoder(os.Stdout)
    enc.SetEscapeHTML(false)
    enc.SetIndent("", "  ")
    return enc.Encode(doc)
}

func cmdVerifyAddedNodes(args []string) error {
    if len(args) != 4 {
        return errors.New("verify-added-nodes <nodes.json> <port-map> <ss-output> <journal-log>")
    }

    var nodes []any
    if err := readJSONFile(args[0], &nodes); err != nil {
        return err
    }

    portMap, err := readPortMap(args[1])
    if err != nil {
        return err
    }

    ssBytes, err := os.ReadFile(args[2])
    if err != nil {
        return err
    }
    journalBytes, err := os.ReadFile(args[3])
    if err != nil {
        return err
    }

    ssText := string(ssBytes)
    journalLines := strings.Split(string(journalBytes), "\n")

    failed := false

    containsPort := func(port string) bool {
        if port == "" {
            return false
        }
        return strings.Contains(ssText, ":"+port)
    }

    containsAny := func(s string, subs ...string) bool {
        s = strings.ToLower(s)
        for _, sub := range subs {
            if strings.Contains(s, strings.ToLower(sub)) {
                return true
            }
        }
        return false
    }

    findNodeLogLines := func(nid string) []string {
        out := make([]string, 0, 8)
        for _, line := range journalLines {
            l := strings.ToLower(line)
            if strings.Contains(l, "nodeid:"+nid) ||
                strings.Contains(l, "nodeid "+nid) ||
                strings.Contains(l, "node "+nid) ||
                strings.Contains(l, ":"+nid+"]") ||
                strings.Contains(l, ":"+nid+"\"") ||
                strings.Contains(l, ":"+nid+" ") ||
                strings.Contains(l, "-vless:"+nid) ||
                strings.Contains(l, "-vmess:"+nid) ||
                strings.Contains(l, "-trojan:"+nid) ||
                strings.Contains(l, "-shadowsocks:"+nid) ||
                strings.Contains(l, "["+nid+"]") {
                out = append(out, line)
            }
        }
        return out
    }

    for _, n := range nodes {
        m, ok := n.(map[string]any)
        if !ok {
            continue
        }

        nid := strconv.Itoa(nodeID(n))
        if nid == "0" {
            continue
        }

        apiHost := str(m["ApiHost"])
        port := portMap[nid]
        if port == "" {
            if p := m["Port"]; p != nil {
                switch x := p.(type) {
                case float64:
                    port = strconv.Itoa(int(x))
                case string:
                    port = x
                case json.Number:
                    port = x.String()
                }
            }
        }

        nodeLogs := findNodeLogLines(nid)
        nodeLogText := strings.ToLower(strings.Join(nodeLogs, "\n"))

        hasPort := containsPort(port)
        hasSuccessLog := containsAny(nodeLogText,
            "added 1 new users",
            "added new users",
            "start monitor node status",
            "start report node status",
            "node online",
        )

        if hasPort && hasSuccessLog && !containsAny(nodeLogText, "existing tag found") {
            fmt.Printf("OK 节点 %s 监听端口 %s 已启动，日志已确认绑定成功\n", nid, port)
            continue
        }

        if containsAny(nodeLogText, "existing tag found") {
            if hasPort {
                fmt.Printf("WARN 节点 %s 监听端口 %s 已启动，但检测到重复注册同一 tag\n", nid, port)
                fmt.Printf("HINT 节点 %s 已成功添加一次，后续重复 add inbound 被内核拒绝\n", nid)
                fmt.Printf("FIX 节点 %s 若业务正常可先忽略；若想消除日志，请排查 V2bX 是否对同一 NodeID 重复注册\n", nid)
                continue
            } else {
                fmt.Printf("FAIL 节点 %s 监听端口 %s 未监听\n", nid, port)
                fmt.Printf("HINT 节点 %s 检测到重复注册同一 tag，且端口未起来\n", nid)
                fmt.Printf("FIX 节点 %s 请检查是否存在相同 NodeID/相同 tag 的重复注册逻辑\n", nid)
                failed = true
                continue
            }
        }

        if hasPort {
            fmt.Printf("OK 节点 %s 监听端口 %s 已启动\n", nid, port)
            if len(nodeLogs) == 0 {
                fmt.Printf("WARN 节点 %s 当前未抓到专属成功日志，可能只是日志窗口较短\n", nid)
            }
            continue
        }

        if containsAny(nodeLogText,
            "server does not exist",
            "not found",
            "invalid",
            "unauthorized",
            "forbidden",
            "get node info error",
            "start node controller",
            "run nodes failed",
        ) {
            fmt.Printf("FAIL 节点 %s 监听端口 %s 未监听\n", nid, port)
            fmt.Printf("HINT 节点 %s 疑似面板下发失败或节点不存在\n", nid)
            fmt.Printf("FIX 节点 %s 请检查面板里 NodeID=%s 是否真实存在，类型是否与脚本选择一致（如 xray/vless/reality）\n", nid, nid)
            if apiHost != "" {
                fmt.Printf("FIX 节点 %s 请检查面板地址/API Key 是否正确：%s\n", nid, apiHost)
            }
            failed = true
            continue
        }

        fmt.Printf("FAIL 节点 %s 监听端口 %s 未监听\n", nid, port)
        fmt.Printf("HINT 节点 %s 当前未抓到明确专属错误日志，更像是启动延迟、端口冲突或核心尚未完成绑定\n", nid)
        fmt.Printf("FIX 节点 %s 可执行：ss -lntp | grep %s\n", nid, port)
        fmt.Printf("FIX 节点 %s 可执行：journalctl -u python -n 120 --no-pager | grep -i '%s\\|%s'\n", nid, nid, port)
        failed = true
    }

    if failed {
        return errors.New("verification failed")
    }
    return nil
}
func cmdSendTelegram(args []string) error {
    if len(args) < 3 { return errors.New("send-telegram <token> <chat_id> <message> [parse_mode] [reply_markup_json]") }
    token, chatID, msg := args[0], args[1], args[2]
    parseMode := "HTML"
    if len(args) >= 4 && args[3] != "" { parseMode = args[3] }
    form := url.Values{}
    form.Set("chat_id", chatID)
    form.Set("text", msg)
    form.Set("parse_mode", parseMode)
    if len(args) >= 5 && args[4] != "" { form.Set("reply_markup", args[4]) }
    endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
    resp, err := http.PostForm(endpoint, form)
    if err != nil { return err }
    defer resp.Body.Close()
    body, _ := io.ReadAll(resp.Body)
    if resp.StatusCode/100 != 2 { return fmt.Errorf("telegram http %d: %s", resp.StatusCode, string(body)) }
    fmt.Print(string(body))
    return nil
}

func nodeID(v any) int {
    if m, ok := v.(map[string]any); ok {
        switch x := m["NodeID"].(type) {
        case float64:
            return int(x)
        case string:
            i, _ := strconv.Atoi(x)
            return i
        case json.Number:
            i, _ := strconv.Atoi(x.String())
            return i
        }
    }
    return 0
}

func readPortMap(p string) (map[string]string, error) {
    f, err := os.Open(p)
    if err != nil { return nil, err }
    defer f.Close()
    out := map[string]string{}
    s := bufio.NewScanner(f)
    for s.Scan() {
        line := strings.TrimSpace(s.Text())
        if line == "" { continue }
        parts := strings.Split(line, ":")
        if len(parts) >= 2 { out[parts[0]] = parts[1] }
    }
    return out, s.Err()
}

func readJSONFile(path string, out any) error {
    b, err := os.ReadFile(path)
    if err != nil { return err }
    dec := json.NewDecoder(bytes.NewReader(b))
    dec.UseNumber()
    return dec.Decode(out)
}
func writeJSONFile(path string, v any) error {
    b, err := json.MarshalIndent(v, "", "  ")
    if err != nil { return err }
    b = append(b, '\n')
    return os.WriteFile(path, b, 0644)
}
func b64DecodeAuto(s string) ([]byte, error) {
    s = strings.TrimSpace(s)
    s = strings.ReplaceAll(s, "-", "+")
    s = strings.ReplaceAll(s, "_", "/")
    if m := len(s) % 4; m != 0 { s += strings.Repeat("=", 4-m) }
    return base64.StdEncoding.DecodeString(s)
}
func defaultIfEmpty(v, d string) string { if v == "" { return d }; return v }
func firstNonEmpty(vs ...string) string { for _, v := range vs { if v != "" { return v } }; return "" }
func ternary[T any](cond bool, a, b T) T { if cond { return a }; return b }
func str(v any) string {
    // --- 核心修复：如果值为 nil，直接返回空字符串，不要返回 "<nil>" ---
    if v == nil {
        return ""
    }
    // -------------------------------------------------------
    switch x := v.(type) {
    case string: return x
    case json.Number: return x.String()
    case float64: return strconv.FormatFloat(x, 'f', -1, 64)
    default: return fmt.Sprint(x)
    }
}
func splitHostPortMaybeIPv6(s string) (string, string, error) {
    if strings.HasPrefix(s, "[") {
        host, port, err := net.SplitHostPort(s)
        return strings.Trim(host, "[]"), port, err
    }
    host, port, err := net.SplitHostPort(s)
    if err == nil { return host, port, nil }
    parts := strings.Split(s, ":")
    if len(parts) >= 2 { return parts[0], parts[len(parts)-1], nil }
    return "", "", err
}


func cmdListNodes(args []string) error {
    if len(args) != 1 { return errors.New("list-nodes <config.json>") }
    var doc map[string]any
    if err := readJSONFile(args[0], &doc); err != nil { return err }

    type row struct {
        id    int
        idRaw string
        core  string
        ntype string
        port  string
        host  string
    }

    seen := map[string]bool{}
    rows := []row{}

    addNode := func(n map[string]any, coreHint string) {
        idv, ok := n["NodeID"]
        if !ok { return }
        idRaw := jqStr(idv)
        if idRaw == "" { return }
        if seen[idRaw] { return }
        seen[idRaw] = true

        core := firstNonEmpty(jqStr(n["Core"]), coreHint, "-")
        ntype := firstNonEmpty(jqStr(n["NodeType"]), jqStr(n["Type"]), "-")
        port := firstNonEmpty(jqStr(n["Port"]), "-")
        host := firstNonEmpty(jqStr(n["ApiHost"]), "-")
        rows = append(rows, row{id: jqInt(idv), idRaw: idRaw, core: core, ntype: ntype, port: port, host: host})
    }

    for _, n := range jqArr(doc["Nodes"]) {
        addNode(jqMap(n), firstNonEmpty(jqStr(jqMap(n)["Core"]), "-"))
    }
    for _, c := range jqArr(doc["Cores"]) {
        cm := jqMap(c)
        coreType := firstNonEmpty(jqStr(cm["Type"]), "-")
        for _, n := range jqArr(cm["Nodes"]) {
            addNode(jqMap(n), coreType)
        }
    }

    sort.Slice(rows, func(i, j int) bool {
        if rows[i].id == rows[j].id { return rows[i].idRaw < rows[j].idRaw }
        return rows[i].id < rows[j].id
    })

    for _, r := range rows {
        fmt.Printf("%s\t%s\t%s\t%s\t%s\n", r.idRaw, r.core, r.ntype, r.port, r.host)
    }
    return nil
}

func cmdDeleteNodes(args []string) error {
    if len(args) < 2 { return errors.New("delete-nodes <config.json> <id> [id ...]") }
    path := args[0]
    ids := map[string]bool{}
    for _, id := range args[1:] {
        id = strings.TrimSpace(id)
        if id != "" { ids[id] = true }
    }
    var doc map[string]any
    if err := readJSONFile(path, &doc); err != nil { return err }

    filterNodes := func(nodes []any) []any {
        out := make([]any, 0, len(nodes))
        for _, n := range nodes {
            nm := jqMap(n)
            if ids[jqStr(nm["NodeID"])] { continue }
            out = append(out, n)
        }
        return out
    }

    if nodes, ok := doc["Nodes"].([]any); ok {
        doc["Nodes"] = filterNodes(nodes)
    }
    if cores, ok := doc["Cores"].([]any); ok {
        for i, c := range cores {
            cm := jqMap(c)
            if nodes, ok := cm["Nodes"].([]any); ok {
                cm["Nodes"] = filterNodes(nodes)
                cores[i] = cm
            }
        }
        doc["Cores"] = cores
    }

    return writeJSONFile(path, doc)
}

func cmdListProxyOutbounds(args []string) error {
    if len(args) != 1 { return errors.New("list-proxy-outbounds <custom_outbound.json>") }
    var arr []any
    if err := readJSONFile(args[0], &arr); err != nil { return err }
    for _, it := range arr {
        m := jqMap(it)
        tag := jqStr(m["tag"])
        if !strings.HasPrefix(tag, "proxy_") { continue }
        name := strings.TrimPrefix(tag, "proxy_")
        addr, port := jqOutboundTarget(m)
        if strings.Contains(addr, ":") && !strings.HasPrefix(addr, "[") {
            addr = "[" + addr + "]"
        }
        target := addr + ":" + port
        fmt.Printf("%s\t%s\t%s\n", tag, name, target)
    }
    return nil
}


func cmdJQCompat(args []string) error {
    ja, err := parseJQArgs(args)
    if err != nil { return err }
    f := strings.TrimSpace(strings.Join(strings.Fields(strings.TrimSpace(ja.filter)), " "))
    if ja.rawInput && f == "." {
        b, _ := io.ReadAll(os.Stdin)
        txt := strings.ReplaceAll(string(b), "\r\n", "\n")
        txt = strings.TrimRight(txt, "\n")
        if txt == "" { return nil }
        out := []any{}
        for _, line := range strings.Split(txt, "\n") { out = append(out, line) }
        return jqWrite(out, false, true)
    }
    if ja.slurp && f == "." {
        dec := json.NewDecoder(os.Stdin)
        dec.UseNumber()
        arr := []any{}
        for {
            var v any
            if err := dec.Decode(&v); err != nil {
                if errors.Is(err, io.EOF) { break }
                return err
            }
            arr = append(arr, v)
        }
        return jqWrite(arr, false, true)
    }
    if ja.nullInput { return jqNull(ja, f) }
    data, err := jqRead(ja.file)
    if err != nil { return err }
    out, ok, err := jqApply(ja, f, data)
    if err != nil { return err }
    if ja.exitStatus {
        if ok { return nil }
        return errors.New("no match")
    }
    return jqWrite(out, ja.raw, ja.compact)
}

type jqArgs struct {
    raw bool
    compact bool
    nullInput bool
    slurp bool
    rawInput bool
    exitStatus bool
    filter string
    file string
    vars map[string]string
    jsonVars map[string]any
}

func parseJQArgs(args []string) (*jqArgs, error) {
    ja := &jqArgs{vars: map[string]string{}, jsonVars: map[string]any{}}
    for i := 0; i < len(args); i++ {
        a := args[i]
        switch a {
        case "-r":
            ja.raw = true
        case "-c":
            ja.compact = true
        case "-n":
            ja.nullInput = true
        case "-s":
            ja.slurp = true
        case "-R":
            ja.rawInput = true
        case "-e":
            ja.exitStatus = true
        case "--arg":
            if i+2 >= len(args) { return nil, errors.New("--arg") }
            ja.vars[args[i+1]] = args[i+2]
            i += 2
        case "--argjson":
            if i+2 >= len(args) { return nil, errors.New("--argjson") }
            var v any
            dec := json.NewDecoder(strings.NewReader(args[i+2]))
            dec.UseNumber()
            if err := dec.Decode(&v); err != nil { return nil, err }
            ja.jsonVars[args[i+1]] = v
            i += 2
        case "--slurpfile":
            if i+2 >= len(args) { return nil, errors.New("--slurpfile") }
            v, err := jqRead(args[i+2])
            if err != nil { return nil, err }
            ja.jsonVars[args[i+1]] = []any{v}
            i += 2
        default:
            if strings.HasPrefix(a, "-") { continue }
            if ja.filter == "" { ja.filter = a } else if ja.file == "" { ja.file = a }
        }
    }
    return ja, nil
}

func jqRead(file string) (any, error) {
    var b []byte
    var err error
    if file != "" { b, err = os.ReadFile(file) } else { b, err = io.ReadAll(os.Stdin) }
    if err != nil { return nil, err }
    dec := json.NewDecoder(bytes.NewReader(b))
    dec.UseNumber()
    var v any
    if err := dec.Decode(&v); err != nil { return nil, err }
    return v, nil
}

func jqWrite(v any, raw, compact bool) error {
    if raw {
        switch t := v.(type) {
        case []any:
            for i, x := range t {
                if i > 0 { fmt.Print("\n") }
                _ = jqWrite(x, true, false)
            }
            return nil
        case string:
            fmt.Print(t)
            return nil
        case json.Number:
            fmt.Print(t.String())
            return nil
        case float64:
            if t == float64(int64(t)) { fmt.Print(strconv.FormatInt(int64(t), 10)) } else { fmt.Print(t) }
            return nil
        case nil:
            fmt.Print("null")
            return nil
        default:
            fmt.Print(fmt.Sprintf("%v", t))
            return nil
        }
    }
    if compact {
        b, _ := json.Marshal(v)
        fmt.Print(string(b))
        return nil
    }
    enc := json.NewEncoder(os.Stdout)
    enc.SetEscapeHTML(false)
    return enc.Encode(v)
}

func jqNull(ja *jqArgs, f string) error {
    v, j := ja.vars, ja.jsonVars
    switch f {
    case `{"type":"field", "outboundTag":$ot}`, `{"type":"field", "outboundTag": $ot}`:
        return jqWrite(map[string]any{"type":"field", "outboundTag":v["ot"]}, false, ja.compact)
    case `{"path":$p,"headers":{"Host":$h}}`:
        return jqWrite(map[string]any{"path":v["p"], "headers":map[string]any{"Host":v["h"]}}, false, ja.compact)
    case `[{"user":$u,"pass":$p}]`:
        return jqWrite([]any{map[string]any{"user":v["u"], "pass":v["p"]}}, false, ja.compact)
    case `{"type":"field", "inboundTag":$tags, "outboundTag":$t}`:
        return jqWrite(map[string]any{"type":"field", "inboundTag":j["tags"], "outboundTag":v["t"]}, false, ja.compact)
    case `{"type":"field", "port":($p|tonumber), "outboundTag":$t}`:
        p, _ := strconv.Atoi(v["p"])
        return jqWrite(map[string]any{"type":"field", "port":p, "outboundTag":v["t"]}, false, ja.compact)
    case `{"type":"field", "outboundTag":$t, "network":"tcp,udp"}`:
        return jqWrite(map[string]any{"type":"field", "outboundTag":v["t"], "network":"tcp,udp"}, false, ja.compact)
    case `{ "tag": $t, "protocol": $pr, "settings": $st, "streamSettings": $ss }`:
        return jqWrite(map[string]any{"tag":v["t"], "protocol":v["pr"], "settings":j["st"], "streamSettings":j["ss"]}, false, ja.compact)
    case `{"tag":$t, "protocol":$pr, "settings":{"servers":[{"address":$s, "port":($p|tonumber), "users":$a}]}}`:
        p, _ := strconv.Atoi(v["p"])
        return jqWrite(map[string]any{"tag":v["t"], "protocol":v["pr"], "settings":map[string]any{"servers":[]any{map[string]any{"address":v["s"], "port":p, "users":j["a"]}}}}, false, ja.compact)
    case `{ "tag": "proxy", "protocol": $pr, "settings": { "servers": [{ "address": $s, "port": ($p | tonumber), "users": $a }] } }`:
        p, _ := strconv.Atoi(v["p"])
        return jqWrite(map[string]any{"tag":"proxy", "protocol":v["pr"], "settings":map[string]any{"servers":[]any{map[string]any{"address":v["s"], "port":p, "users":j["a"]}}}}, false, ja.compact)
    case `{ "tag": "proxy_out", "protocol": $pr, "settings": { "servers": [{ "address": $s, "port": ($p | tonumber), "users": $a }] } }`:
        p, _ := strconv.Atoi(v["p"])
        return jqWrite(map[string]any{"tag":"proxy_out", "protocol":v["pr"], "settings":map[string]any{"servers":[]any{map[string]any{"address":v["s"], "port":p, "users":j["a"]}}}}, false, ja.compact)
    case `{"network":$net}`:
        return jqWrite(map[string]any{"network":v["net"]}, false, ja.compact)
    case `{"servers":[{"address":$add,"port":($port|tonumber),"password":$pwd}]}`:
        p, _ := strconv.Atoi(v["port"])
        return jqWrite(map[string]any{"servers":[]any{map[string]any{"address":v["add"], "port":p, "password":v["pwd"]}}}, false, ja.compact)
    case `{"network":"tcp","security":"tls","tlsSettings":{"serverName":$sni}}`:
        return jqWrite(map[string]any{"network":"tcp", "security":"tls", "tlsSettings":map[string]any{"serverName":v["sni"]}}, false, ja.compact)
    }
    // more flexible matches
    if strings.Contains(f, `{"vnext":[{"address":$add,"port":($port|tonumber),"users":[{"id":$id`) {
        p, _ := strconv.Atoi(v["port"])
        user := map[string]any{"id":v["id"]}
        if enc := v["enc"]; enc != "" { user["encryption"] = enc }
        if fl := v["fl"]; fl != "" { user["flow"] = fl }
        if _, ok := user["encryption"]; !ok { user["security"] = "auto" }
        return jqWrite(map[string]any{"vnext":[]any{map[string]any{"address":v["add"], "port":p, "users":[]any{user}}}}, false, ja.compact)
    }
    if strings.Contains(f, `{"servers":[{"address":$add,"port":($port|tonumber),"method":$m,"password":$p`) {
        p, _ := strconv.Atoi(v["port"])
        server := map[string]any{"address":v["add"], "port":p, "method":v["m"], "password":v["p"]}
        if plugin := v["plugin"]; plugin != "" { server["plugin"] = plugin }
        if opts := v["plugin_opts"]; opts != "" { server["pluginOpts"] = opts }
        return jqWrite(map[string]any{"servers":[]any{server}}, false, ja.compact)
    }
    if strings.Contains(f, `{"servers":[{"address":$add,"port":($port|tonumber),"users":[{"user":$u,"pass":$p}]`) {
        p, _ := strconv.Atoi(v["port"])
        return jqWrite(map[string]any{"servers":[]any{map[string]any{"address":v["add"], "port":p, "users":[]any{map[string]any{"user":v["u"], "pass":v["p"]}}}}}, false, ja.compact)
    }
    if strings.Contains(f, `{"servers":[{"address":$add,"port":($port|tonumber)}]`) {
        p, _ := strconv.Atoi(v["port"])
        return jqWrite(map[string]any{"servers":[]any{map[string]any{"address":v["add"], "port":p}}}, false, ja.compact)
    }
    if strings.Contains(f, `{"network":$net,"security":$sec,"realitySettings"`) {
        out := map[string]any{"network":v["net"], "security":v["sec"], "realitySettings":map[string]any{"serverName":v["sni"]}}
        rs := out["realitySettings"].(map[string]any)
        if pbk := v["pbk"]; pbk != "" { rs["publicKey"] = pbk }
        if sid := v["sid"]; sid != "" { rs["shortId"] = sid }
        if fp := v["fp"]; fp != "" { rs["fingerprint"] = fp }
        if ws, ok := j["ws"]; ok { out["wsSettings"] = ws }
        return jqWrite(out, false, ja.compact)
    }
    if strings.Contains(f, `{"network":$net,"security":$sec,"tlsSettings":{"serverName":$sni}}`) {
        out := map[string]any{"network":v["net"], "security":v["sec"], "tlsSettings":map[string]any{"serverName":v["sni"]}}
        if ws, ok := j["ws"]; ok { out["wsSettings"] = ws }
        return jqWrite(out, false, ja.compact)
    }
    if strings.Contains(f, `{"network":$net,"security":$sec}`) {
        out := map[string]any{"network":v["net"], "security":v["sec"]}
        if ws, ok := j["ws"]; ok { out["wsSettings"] = ws }
        return jqWrite(out, false, ja.compact)
    }
    return fmt.Errorf("jq-compat null unsupported: %s", f)
}

func jqApply(ja *jqArgs, f string, data any) (any, bool, error) {
    // generic direct pass-through for validation
    if f == "." { return data, true, nil }
    switch f {
    case `.Cores[]? | select((.Type == "sspanel" or .Type == "sing" or .Type == "xray") and (.OriginalPath // "") != "") | .OriginalPath`:
        obj := jqMap(data)
        for _, c := range jqArr(obj["Cores"]) {
            m := jqMap(c)
            t := jqStr(m["Type"])
            op := jqStr(m["OriginalPath"])
            if (t == "sspanel" || t == "sing" || t == "xray") && op != "" { return op, true, nil }
        }
        return nil, false, nil
    case `.Nodes[] | "\(.NodeID)\t\(.NodeType)\t\(.ApiHost)"`:
        out := []any{}
        for _, n := range jqArr(jqMap(data)["Nodes"]) {
            m := jqMap(n)
            out = append(out, fmt.Sprintf("%v\t%v\t%v", m["NodeID"], m["NodeType"], m["ApiHost"]))
        }
        return out, len(out) > 0, nil
    case `.Nodes |= map(select(.NodeID as $id | ($ids | index($id) == null)))`:
        obj := jqClone(jqMap(data))
        ids := map[string]bool{}
        for _, x := range jqArr(ja.jsonVars["ids"]) { ids[jqStr(x)] = true }
        ns := []any{}
        for _, n := range jqArr(obj["Nodes"]) {
            if !ids[jqStr(jqMap(n)["NodeID"])] { ns = append(ns, n) }
        }
        obj["Nodes"] = ns
        return obj, true, nil
    case `. + [$node]`:
        arr := jqArr(data)
        arr = append(arr, ja.jsonVars["node"])
        return arr, true, nil
    case `.[] | select(.tag == $t)`:
        for _, it := range jqArr(data) {
            if jqStr(jqMap(it)["tag"]) == ja.vars["t"] { return it, true, nil }
        }
        return nil, false, nil
    case `.rules[] | select(.inboundTag != null) | .outboundTag as $o | .inboundTag[] | "\($o)|\(.)"`:
        out := []any{}
        for _, r := range jqArr(jqMap(data)["rules"]) {
            rm := jqMap(r)
            if rm["inboundTag"] == nil { continue }
            for _, inn := range jqArr(rm["inboundTag"]) { out = append(out, fmt.Sprintf("%s|%s", jqStr(rm["outboundTag"]), jqStr(inn))) }
        }
        return out, len(out) > 0, nil
    case `.[] | select(.tag | startswith("proxy_")) | .tag`, `.[] | select(.tag? | startswith("proxy_")) | .tag`:
        out := []any{}
        for _, it := range jqArr(data) {
            tg := jqStr(jqMap(it)["tag"])
            if strings.HasPrefix(tg, "proxy_") { out = append(out, tg) }
        }
        return out, len(out) > 0, nil
    case `.Cores[0].Nodes // .Nodes | sort_by(.NodeID | tonumber) | .[] | "[\(.ApiHost)]-\(.NodeType):\(.NodeID)"`:
        nodes := jqNodes(data)
        sort.Slice(nodes, func(i, j int) bool { return jqInt(jqMap(nodes[i])["NodeID"]) < jqInt(jqMap(nodes[j])["NodeID"]) })
        out := []any{}
        for _, n := range nodes {
            m := jqMap(n)
            out = append(out, fmt.Sprintf("[%s]-%s:%s", jqStr(m["ApiHost"]), jqStr(m["NodeType"]), jqStr(m["NodeID"])))
        }
        return out, len(out) > 0, nil
    case `. + {"network":"tcp,udp"}`:
        m := jqClone(jqMap(data)); m["network"] = "tcp,udp"; return m, true, nil
    case `. + {"domain":[$v]}`:
        m := jqClone(jqMap(data)); m["domain"] = []any{ja.vars["v"]}; return m, true, nil
    case `. + {"inboundTag":$tags}`:
        m := jqClone(jqMap(data)); m["inboundTag"] = ja.jsonVars["tags"]; return m, true, nil
    case `.rules |= map(select(.port != $p))`:
        obj := jqClone(jqMap(data)); p := jqStr(ja.jsonVars["p"]); nr := []any{}
        for _, r := range jqArr(obj["rules"]) { if jqStr(jqMap(r)["port"]) != p { nr = append(nr, r) } }
        obj["rules"] = nr; return obj, true, nil
    case `map(select(.tag != $t))`:
        out := []any{}
        for _, it := range jqArr(data) { if jqStr(jqMap(it)["tag"]) != ja.vars["t"] { out = append(out, it) } }
        return out, true, nil
    case `.rules |= map(select(.outboundTag != $t))`:
        obj := jqClone(jqMap(data)); nr := []any{}
        for _, it := range jqArr(obj["rules"]) { if jqStr(jqMap(it)["outboundTag"]) != ja.vars["t"] { nr = append(nr, it) } }
        obj["rules"] = nr; return obj, true, nil
    case `.Cores[0].Nodes // .Nodes | .[] | .NodeID`:
        out := []any{}
        for _, n := range jqNodes(data) { out = append(out, jqMap(n)["NodeID"]) }
        return out, len(out) > 0, nil
    case `.rules[] | select(.outboundTag=="block" and .port=="25,465,587")`:
        for _, r := range jqArr(jqMap(data)["rules"]) {
            rm := jqMap(r)
            if jqStr(rm["outboundTag"]) == "block" && jqStr(rm["port"]) == "25,465,587" { return r, true, nil }
        }
        return nil, false, nil
    case `.rules[] | select(.outboundTag==$MAIN_TAG) | .inboundTag[0]`:
        for _, r := range jqArr(jqMap(data)["rules"]) {
            rm := jqMap(r)
            if jqStr(rm["outboundTag"]) == ja.vars["MAIN_TAG"] {
                arr := jqArr(rm["inboundTag"])
                if len(arr) > 0 { return arr[0], true, nil }
            }
        }
        return nil, false, nil
    case `.vnext[0].address // .servers[0].address // .address // "node"`:
        return jqOutboundAddr(data), true, nil
    case `.add`, `.port`, `.id`:
        return jqMap(data)[strings.TrimPrefix(f, ".")], true, nil
    case `.net // "tcp"`:
        return firstNonEmpty(jqStr(jqMap(data)["net"]), "tcp"), true, nil
    case `.path // "/"`:
        return firstNonEmpty(jqStr(jqMap(data)["path"]), "/"), true, nil
    case `.tls // "none"`:
        return firstNonEmpty(jqStr(jqMap(data)["tls"]), "none"), true, nil
    case `.host // ""`, `.sni // ""`:
        return firstNonEmpty(jqStr(jqMap(data)[strings.TrimPrefix(f, ".")[:len(strings.TrimPrefix(f, "."))-6]]), ""), true, nil
    }

    // Pattern/regex based handlers
    if f == `(.rules // []) | map(select(.outboundTag != $t and .ip != ["geoip:private"] and (.network != "tcp,udp" or .outboundTag != "direct")))` {
        obj := jqMap(data)
        out := []any{}
        for _, r := range jqArr(obj["rules"]) {
            rm := jqMap(r)
            if jqStr(rm["outboundTag"]) == ja.vars["t"] { continue }
            ipArr := jqArr(rm["ip"])
            if len(ipArr) == 1 && jqStr(ipArr[0]) == "geoip:private" { continue }
            if jqStr(rm["network"]) == "tcp,udp" && jqStr(rm["outboundTag"]) == "direct" { continue }
            out = append(out, r)
        }
        return out, true, nil
    }
    if strings.Contains(f, `.rules = ([$priv] + $new + ((.rules // []) | map(select(.outboundTag != $t`) {
        obj := jqClone(jqMap(data))
        nr := []any{ja.jsonVars["priv"]}
        nr = append(nr, jqArr(ja.jsonVars["new"])...)
        for _, r := range jqArr(obj["rules"]) {
            rm := jqMap(r)
            if jqStr(rm["outboundTag"]) == ja.vars["t"] { continue }
            ipArr := jqArr(rm["ip"])
            if len(ipArr) == 1 && jqStr(ipArr[0]) == "geoip:private" { continue }
            if jqStr(rm["network"]) == "tcp,udp" && jqStr(rm["outboundTag"]) == "direct" { continue }
            nr = append(nr, r)
        }
        nr = append(nr, ja.jsonVars["fallback"])
        obj["rules"] = nr
        return obj, true, nil
    }
    if strings.Contains(f, `.rules[]? | select(.inboundTag != null and .outboundTag != null)`) && strings.Contains(f, `capture("(?<p>[A-Za-z0-9]+:[0-9]+)")`) {
        out := []any{}
        re := regexp.MustCompile(`([A-Za-z0-9]+:[0-9]+)`)
        for _, r := range jqArr(jqMap(data)["rules"]) {
            rm := jqMap(r)
            if rm["inboundTag"] == nil || rm["outboundTag"] == nil { continue }
            for _, inn := range jqArr(rm["inboundTag"]) {
                s := jqStr(inn)
                p := s
                if m := re.FindStringSubmatch(s); len(m) > 1 { p = m[1] }
                out = append(out, fmt.Sprintf("%s\t%s", jqStr(rm["outboundTag"]), p))
            }
        }
        return out, len(out) > 0, nil
    }
    if strings.Contains(f, `select(.tag? and (.tag | startswith("proxy_")))`) && strings.Contains(f, `@tsv`) {
        out := []any{}
        for _, it := range jqArr(data) {
            m := jqMap(it)
            tg := jqStr(m["tag"])
            if !strings.HasPrefix(tg, "proxy_") { continue }
            name := strings.TrimPrefix(tg, "proxy_")
            addr, port := jqOutboundTarget(m)
            if strings.Contains(f, `(.type // .protocol // "unknown")`) {
                proto := firstNonEmpty(jqStr(m["type"]), jqStr(m["protocol"]), "unknown")
                out = append(out, fmt.Sprintf("%s\t%s\t%s\t%s\t%s", tg, proto, name, addr, port))
            } else {
                out = append(out, fmt.Sprintf("%s\t%s\t%s:%s", tg, name, addr, port))
            }
        }
        return out, len(out) > 0, nil
    }
    if f == `.rules[]? | select(.port != null) | "\(.port)|\(.outboundTag)"` {
        out := []any{}
        for _, r := range jqArr(jqMap(data)["rules"]) {
            rm := jqMap(r); if rm["port"] == nil { continue }
            out = append(out, fmt.Sprintf("%s|%s", jqStr(rm["port"]), jqStr(rm["outboundTag"])))
        }
        return out, len(out) > 0, nil
    }
    if f == `.rules[]? | select(.inboundTag != null and .outboundTag != "direct") | .inboundTag[]` {
        out := []any{}
        for _, r := range jqArr(jqMap(data)["rules"]) {
            rm := jqMap(r); if rm["inboundTag"] == nil || jqStr(rm["outboundTag"]) == "direct" { continue }
            for _, inn := range jqArr(rm["inboundTag"]) { out = append(out, jqStr(inn)) }
        }
        return out, len(out) > 0, nil
    }
    if strings.Contains(f, `.rules |= map(`) && strings.Contains(f, `.inboundTag |= map(select(. != $tag))`) {
        obj := jqClone(jqMap(data)); nr := []any{}
        for _, r := range jqArr(obj["rules"]) {
            rm := jqClone(jqMap(r))
            if rm["inboundTag"] != nil {
                keep := []any{}
                for _, inn := range jqArr(rm["inboundTag"]) { if jqStr(inn) != ja.vars["tag"] { keep = append(keep, inn) } }
                if len(keep) == 0 {
                    delete(rm, "inboundTag")
                } else {
                    rm["inboundTag"] = keep
                }
            }
            nr = append(nr, rm)
        }
        obj["rules"] = nr
        return obj, true, nil
    }
    if strings.Contains(f, `.domainStrategy = "IPIfNonMatch"`) && strings.Contains(f, `.rules = ([$priv, $node] +`) {
        obj := jqClone(jqMap(data)); obj["domainStrategy"] = "IPIfNonMatch"
        nr := []any{ja.jsonVars["priv"], ja.jsonVars["node"]}
        nodeTag := jqStr(jqMap(ja.jsonVars["node"])["outboundTag"])
        for _, r := range jqArr(obj["rules"]) {
            rm := jqMap(r)
            if jqStr(rm["outboundTag"]) == nodeTag { continue }
            ipArr := jqArr(rm["ip"])
            if len(ipArr) == 1 && jqStr(ipArr[0]) == "geoip:private" { continue }
            nr = append(nr, r)
        }
        obj["rules"] = nr
        return obj, true, nil
    }
    if f == `map(select(.tag != "proxy"))` {
        out := []any{}
        for _, it := range jqArr(data) { if jqStr(jqMap(it)["tag"]) != "proxy" { out = append(out, it) } }
        return out, true, nil
    }
    if f == `map(select(.tag != "proxy")) | [$out] + .` || f == `map(select(.tag != "proxy")) | [$out] + .` {
        out := []any{ja.jsonVars["out"]}
        for _, it := range jqArr(data) { if jqStr(jqMap(it)["tag"]) != "proxy" { out = append(out, it) } }
        return out, true, nil
    }
    if strings.Contains(f, `del(.outbounds[] | select(.tag=="proxy_out"))`) {
        obj := jqClone(jqMap(data))
        outb := []any{}
        for _, it := range jqArr(obj["outbounds"]) { if jqStr(jqMap(it)["tag"]) != "proxy_out" { outb = append(outb, it) } }
        obj["outbounds"] = outb
        if routing, ok := obj["routing"].(map[string]any); ok {
            nr := []any{}
            for _, r := range jqArr(routing["rules"]) { if jqStr(jqMap(r)["outboundTag"]) != "proxy_out" { nr = append(nr, r) } }
            routing["rules"] = nr
            obj["routing"] = routing
        }
        return obj, true, nil
    }
    if strings.Contains(f, `.outbounds = [$out] + .outbounds`) {
        obj := jqClone(jqMap(data))
        outb := []any{ja.jsonVars["out"]}
        outb = append(outb, jqArr(obj["outbounds"])...)
        obj["outbounds"] = outb
        routing := jqMap(obj["routing"])
        if len(routing) == 0 { routing = map[string]any{"domainStrategy":"AsIs", "rules":[]any{}} }
        nr := []any{map[string]any{"type":"field", "outboundTag":"proxy_out", "network":"tcp,udp"}}
        nr = append(nr, jqArr(routing["rules"])...)
        routing["rules"] = nr
        obj["routing"] = routing
        return obj, true, nil
    }
    if f == `([.[] | select(.tag == "direct" or .tag == "block")] + [.[] | select((.tag != "direct") and (.tag != "block") and ((.tag | startswith("proxy_")) | not))])` {
        var a1, a2 []any
        for _, it := range jqArr(data) {
            tg := jqStr(jqMap(it)["tag"])
            if tg == "direct" || tg == "block" { a1 = append(a1, it) } else if !strings.HasPrefix(tg, "proxy_") { a2 = append(a2, it) }
        }
        return append(a1, a2...), true, nil
    }
    if strings.Contains(f, `map(select(.tag != $out.tag)) + [$out]`) {
        out := []any{}
        outTag := jqStr(jqMap(ja.jsonVars["out"])["tag"])
        for _, it := range jqArr(data) { if jqStr(jqMap(it)["tag"]) != outTag { out = append(out, it) } }
        out = append(out, ja.jsonVars["out"])
        var direct, rest []any
        for _, it := range out { if jqStr(jqMap(it)["tag"]) == "direct" { direct = append(direct, it) } else { rest = append(rest, it) } }
        return append(direct, rest...), true, nil
    }
    if strings.Contains(f, `.Nodes |= map(.ApiHost = $host | .ApiKey = $key)`) {
        obj := jqClone(jqMap(data)); ns := []any{}
        for _, n := range jqNodes(obj) { m := jqClone(jqMap(n)); m["ApiHost"] = ja.vars["host"]; m["ApiKey"] = ja.vars["key"]; ns = append(ns, m) }
        if cores := jqArr(obj["Cores"]); len(cores) > 0 && jqMap(cores[0])["Nodes"] != nil { c0 := jqClone(jqMap(cores[0])); c0["Nodes"] = ns; cores[0] = c0; obj["Cores"] = cores } else { obj["Nodes"] = ns }
        return obj, true, nil
    }
    if strings.Contains(f, `.rules |= map(select(.outboundTag != "block"`) {
        obj := jqClone(jqMap(data)); nr := []any{}
        for _, r := range jqArr(obj["rules"]) {
            rm := jqMap(r)
            if jqStr(rm["outboundTag"]) == "block" {
                if jqStr(rm["port"]) == "25,465,587" { continue }
                prot := jqArr(rm["protocol"]); if len(prot) == 1 && jqStr(prot[0]) == "bittorrent" { continue }
                ipa := jqArr(rm["ip"]); if len(ipa) == 1 && jqStr(ipa[0]) == "geoip:private" { continue }
            }
            nr = append(nr, r)
        }
        obj["rules"] = nr; return obj, true, nil
    }
    if strings.Contains(f, `.rules = [ {"type":"field", "port":"25,465,587"`) || strings.Contains(f, `.rules = [ {"type":"field", "port":"25,465,587", "outboundTag":"block"}`) || strings.Contains(f, `.rules = [`) && strings.Contains(f, `"protocol":["bittorrent"]`) {
        obj := jqClone(jqMap(data))
        nr := []any{map[string]any{"type":"field", "port":"25,465,587", "outboundTag":"block"}, map[string]any{"type":"field", "protocol":[]any{"bittorrent"}, "outboundTag":"block"}}
        for _, r := range jqArr(obj["rules"]) {
            rm := jqMap(r)
            if jqStr(rm["outboundTag"]) == "block" {
                if jqStr(rm["port"]) == "25,465,587" { continue }
                prot := jqArr(rm["protocol"]); if len(prot) == 1 && jqStr(prot[0]) == "bittorrent" { continue }
            }
            nr = append(nr, r)
        }
        obj["rules"] = nr; return obj, true, nil
    }
    if f == `. + [{"tag": "IPv4-Only", "protocol": "freedom", "settings": { "domainStrategy": "UseIPv4" }}, {"tag": "IPv6-Only", "protocol": "freedom", "settings": { "domainStrategy": "UseIPv6" }}]` {
        arr := jqArr(data)
        has4, has6 := false, false
        for _, it := range arr { tg := jqStr(jqMap(it)["tag"]); if tg == "IPv4-Only" { has4 = true }; if tg == "IPv6-Only" { has6 = true } }
        if !has4 { arr = append(arr, map[string]any{"tag":"IPv4-Only", "protocol":"freedom", "settings":map[string]any{"domainStrategy":"UseIPv4"}}) }
        if !has6 { arr = append(arr, map[string]any{"tag":"IPv6-Only", "protocol":"freedom", "settings":map[string]any{"domainStrategy":"UseIPv6"}}) }
        return arr, true, nil
    }
    if strings.Contains(f, `.rules[]? | select(.outboundTag == "IPv4-Only"`) {
        return jqFindInboundOutbound(data, ja.vars["in"], "IPv4-Only", false), jqFindInboundOutbound(data, ja.vars["in"], "IPv4-Only", false) != nil, nil
    }
    if strings.Contains(f, `.rules[]? | select(.outboundTag == "IPv6-Only"`) {
        return jqFindInboundOutbound(data, ja.vars["in"], "IPv6-Only", false), jqFindInboundOutbound(data, ja.vars["in"], "IPv6-Only", false) != nil, nil
    }
    if strings.Contains(f, `.rules[]? | select(.outboundTag | startswith("bind_"))`) && strings.Contains(f, `.inboundTag | index($in)`) {
        if strings.HasSuffix(f, `| .outboundTag`) {
            r := jqFindInboundPrefix(data, ja.vars["in"], "bind_")
            if r == nil { return nil, false, nil }
            return jqMap(r)["outboundTag"], true, nil
        }
        r := jqFindInboundPrefix(data, ja.vars["in"], "bind_")
        return r, r != nil, nil
    }
    if strings.Contains(f, `.rules[]? | select(.outboundTag | startswith("proxy_"))`) && strings.Contains(f, `.inboundTag | index($in)`) {
        r := jqFindInboundPrefix(data, ja.vars["in"], "proxy_")
        return r, r != nil, nil
    }
    if strings.Contains(f, `. += [{"tag":`) && strings.Contains(f, `"sendThrough":`) {
        re := regexp.MustCompile(`\{\"tag\": \"([^\"]+)\".*\"sendThrough\": \"([^\"]+)\"`)
        m := re.FindStringSubmatch(f)
        if len(m) == 3 {
            arr := jqArr(data)
            arr = append(arr, map[string]any{"tag":m[1], "protocol":"freedom", "sendThrough":m[2], "settings":map[string]any{"domainStrategy":"AsIs"}})
            return arr, true, nil
        }
    }
    if strings.Contains(f, `.rules |= map(`) && strings.Contains(f, `if (.inboundTag != null and (.inboundTag | index($in)))`) {
        obj := jqClone(jqMap(data))
        rules := []any{}
        target := ja.vars["in"]
        for _, r := range jqArr(obj["rules"]) {
            rm := jqClone(jqMap(r))
            if jqHasInbound(rm, target) {
                if rm["domain"] == nil && rm["ip"] == nil && rm["port"] == nil && rm["protocol"] == nil && rm["source"] == nil {
                    keep := []any{}
                    for _, inn := range jqArr(rm["inboundTag"]) { if jqStr(inn) != target { keep = append(keep, inn) } }
                    rm["inboundTag"] = keep
                }
            }
            rules = append(rules, rm)
        }
        filtered := []any{}
        for _, r := range rules {
            rm := jqMap(r)
            keep := rm["inboundTag"] == nil || (len(jqArr(rm["inboundTag"])) > 0) || rm["network"] != nil || rm["ip"] != nil || rm["domain"] != nil
            if keep { filtered = append(filtered, r) }
        }
        if ja.vars["tgt"] != "direct" {
            newRule := map[string]any{"type":"field", "inboundTag":[]any{target}, "outboundTag":ja.vars["tgt"]}
            if len(filtered) > 0 {
                last := filtered[len(filtered)-1]
                filtered = append(filtered[:len(filtered)-1], newRule, last)
            } else {
                filtered = append(filtered, newRule)
            }
        }
        obj["rules"] = filtered
        return obj, true, nil
    }

    // cloudflare tunnel dynamic string filters
    if strings.HasPrefix(f, `.[] | select(.id=="`) {
        id := between(f, `.id=="`, `"`)
        if strings.HasSuffix(f, `.name // "unknown"`) {
            for _, it := range jqArr(data) { m := jqMap(it); if jqStr(m["id"]) == id { name := jqStr(m["name"]); if name == "" { name = "unknown" }; return name, true, nil } }
            return nil, false, nil
        }
        if strings.HasSuffix(f, `(.connections // []) | length`) {
            for _, it := range jqArr(data) { m := jqMap(it); if jqStr(m["id"]) == id { return len(jqArr(m["connections"])), true, nil } }
            return 0, false, nil
        }
        for _, it := range jqArr(data) { if jqStr(jqMap(it)["id"]) == id { return it, true, nil } }
        return nil, false, nil
    }
    if f == `.[] | "\(.name)\t\(.id)\tconnections=\((.connections//[])|length)"` {
        out := []any{}
        for _, it := range jqArr(data) { m := jqMap(it); out = append(out, fmt.Sprintf("%s\t%s\tconnections=%d", jqStr(m["name"]), jqStr(m["id"]), len(jqArr(m["connections"])))) }
        return out, len(out) > 0, nil
    }
    return nil, false, fmt.Errorf("jq-compat unsupported: %s", f)
}

func jqOutboundAddr(v any) string {
    m := jqMap(v)
    if vnext := jqArr(m["vnext"]); len(vnext) > 0 { return jqStr(jqMap(vnext[0])["address"]) }
    if servers := jqArr(m["servers"]); len(servers) > 0 { return jqStr(jqMap(servers[0])["address"]) }
    return firstNonEmpty(jqStr(m["address"]), "node")
}
func jqOutboundTarget(m map[string]any) (string, string) {
    if server := jqStr(m["server"]); server != "" { return server, jqStr(m["server_port"]) }
    settings := jqMap(m["settings"])
    if vnext := jqArr(settings["vnext"]); len(vnext) > 0 { vm := jqMap(vnext[0]); return firstNonEmpty(jqStr(vm["address"]), "null"), jqStr(vm["port"]) }
    if servers := jqArr(settings["servers"]); len(servers) > 0 { sm := jqMap(servers[0]); return firstNonEmpty(jqStr(sm["address"]), "null"), jqStr(sm["port"]) }
    if servers := jqArr(m["servers"]); len(servers) > 0 { sm := jqMap(servers[0]); return firstNonEmpty(jqStr(sm["address"]), "null"), jqStr(sm["port"]) }
    return "null", "0"
}
func jqFindInboundOutbound(data any, inbound string, outbound string, prefix bool) any {
    for _, r := range jqArr(jqMap(data)["rules"]) {
        rm := jqMap(r)
        ot := jqStr(rm["outboundTag"])
        if prefix { if !strings.HasPrefix(ot, outbound) { continue } } else { if ot != outbound { continue } }
        if jqHasInbound(rm, inbound) { return r }
    }
    return nil
}
func jqFindInboundPrefix(data any, inbound string, prefix string) any { return jqFindInboundOutbound(data, inbound, prefix, true) }
func jqHasInbound(rule map[string]any, inbound string) bool { for _, inn := range jqArr(rule["inboundTag"]) { if jqStr(inn) == inbound { return true } }; return false }
func between(s, a, b string) string { i:=strings.Index(s,a); if i<0 { return "" }; i+=len(a); j:=strings.Index(s[i:],b); if j<0 { return "" }; return s[i:i+j] }
func jqNodes(v any) []any { obj:=jqMap(v); if cores:=jqArr(obj["Cores"]); len(cores)>0 { if ns:=jqArr(jqMap(cores[0])["Nodes"]); len(ns)>0 { return ns } }; return jqArr(obj["Nodes"]) }
func jqMap(v any) map[string]any { if m,ok:=v.(map[string]any); ok { return m }; return map[string]any{} }
func jqArr(v any) []any { if a,ok:=v.([]any); ok { return a }; return nil }
func jqClone(m map[string]any) map[string]any { b,_:=json.Marshal(m); out:=map[string]any{}; _=json.Unmarshal(b,&out); return out }
func jqStr(v any) string { switch t:=v.(type){ case nil:return ""; case string:return t; case json.Number:return t.String(); case float64: if t==float64(int64(t)) { return strconv.FormatInt(int64(t),10) }; return fmt.Sprintf("%v",t); default:return fmt.Sprintf("%v",t) } }
func jqInt(v any) int { switch t:=v.(type){ case int:return t; case int64:return int(t); case float64:return int(t); case json.Number:i,_:=t.Int64(); return int(i); case string:i,_:=strconv.Atoi(t); return i; default:return 0 } }
// trigger build
