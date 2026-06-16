//! C AST analysis via tree-sitter.
//!
//! Parses C source code into an AST and detects POSIX patterns.

use crate::patterns::{PatternMatch, PosixPattern};
use tree_sitter::Parser;

/// C source code analyzer using tree-sitter.
pub struct CAnalyzer {
    parser: Parser,
}

impl CAnalyzer {
    /// Create a new C analyzer.
    pub fn new() -> Self {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_c::language())
            .expect("Failed to set tree-sitter C language");
        Self { parser }
    }

    /// Analyze C source code and return all detected POSIX patterns.
    pub fn analyze(&mut self, source: &str) -> Vec<PatternMatch> {
        let tree = self.parser.parse(source, None).expect("Failed to parse C source");
        let root = tree.root_node();
        let mut matches = Vec::new();
        self.walk_node(source, root, &mut matches);
        matches.sort_by_key(|m| m.line);
        matches
    }

    fn walk_node(&self, source: &str, node: tree_sitter::Node, matches: &mut Vec<PatternMatch>) {
        if let Some(call_match) = self.match_call_node(source, node) {
            matches.push(call_match);
        }
        for i in 0..node.child_count() {
            self.walk_node(source, node.child(i).unwrap(), matches);
        }
    }

    fn match_call_node(&self, source: &str, node: tree_sitter::Node) -> Option<PatternMatch> {
        if node.kind() != "call_expression" {
            return None;
        }

        let func_node = node.child(0)?;
        let func_name = func_node.utf8_text(source.as_bytes()).ok()?;

        let line = node.start_position().row + 1;

        let pattern = match func_name {
            "socket" => {
                // Check if we can determine TCP vs UDP from arguments
                let args = self.get_call_args(source, node);
                if let Some(first_arg) = args.first() {
                    if first_arg.contains("SOCK_STREAM") {
                        PosixPattern::SocketTcp
                    } else if first_arg.contains("SOCK_DGRAM") {
                        PosixPattern::SocketUdp
                    } else {
                        PosixPattern::SocketTcp // default to TCP
                    }
                } else {
                    PosixPattern::SocketTcp
                }
            }
            "bind" => PosixPattern::Bind,
            "listen" => PosixPattern::Listen,
            "accept" | "accept4" => PosixPattern::Accept,
            "connect" => PosixPattern::Connect,
            "recv" | "read" => PosixPattern::Recv,
            "send" | "write" => PosixPattern::Send,
            "gethostbyname" | "getaddrinfo" | "gethostbyaddr" => PosixPattern::GetHostByName,
            "close" => PosixPattern::Close,
            "fopen" | "fopen_s" => PosixPattern::Fopen,
            "fread" => PosixPattern::Fread,
            "fwrite" => PosixPattern::Fwrite,
            "fclose" => PosixPattern::Fclose,
            "poll" => PosixPattern::Poll,
            "select" => PosixPattern::Select,
            "fork" | "vfork" => PosixPattern::Fork,
            "exec" | "execve" | "execl" | "execvp" => PosixPattern::Exec,
            "socketpair" => PosixPattern::SocketPair,
            "shutdown" => PosixPattern::Shutdown,
            _ => return None,
        };

        let snippet = node.utf8_text(source.as_bytes()).ok()?.to_string();
        let transformability = pattern.transformability();
        let start_byte = node.start_byte();
        let end_byte = node.end_byte();

        Some(PatternMatch {
            line,
            start_byte,
            end_byte,
            pattern,
            snippet,
            transformability,
        })
    }

    fn get_call_args(&self, source: &str, node: tree_sitter::Node) -> Vec<String> {
        let mut args = Vec::new();
        // The call_expression structure: function arg1 arg2 ...
        // child(0) = function, child(1..) = arguments
        let _cursor = node.walk();
        for i in 1..node.child_count() {
            if let Some(arg_node) = node.child(i) {
                let arg_text = arg_node.utf8_text(source.as_bytes()).unwrap_or("").to_string();
                args.push(arg_text);
            }
        }
        args
    }
}

impl Default for CAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_socket_tcp() {
        let mut analyzer = CAnalyzer::new();
        let source = r#"
int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    return 0;
}
"#;
        let matches = analyzer.analyze(source);
        assert!(matches.iter().any(|m| matches!(m.pattern, PosixPattern::SocketTcp)));
    }

    #[test]
    fn test_detect_socket_udp() {
        let mut analyzer = CAnalyzer::new();
        let source = r#"
int main() {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    return 0;
}
"#;
        let matches = analyzer.analyze(source);
        assert!(matches.iter().any(|m| matches!(m.pattern, PosixPattern::SocketUdp)));
    }

    #[test]
    fn test_detect_bind_listen_accept() {
        let mut analyzer = CAnalyzer::new();
        let source = r#"
int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    bind(fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(fd, 128);
    int client = accept(fd, NULL, NULL);
    return 0;
}
"#;
        let matches = analyzer.analyze(source);
        let patterns: Vec<_> = matches.iter().map(|m| m.pattern.clone()).collect();
        assert!(patterns.contains(&PosixPattern::Bind));
        assert!(patterns.contains(&PosixPattern::Listen));
        assert!(patterns.contains(&PosixPattern::Accept));
    }

    #[test]
    fn test_detect_poll_not_transformable() {
        let mut analyzer = CAnalyzer::new();
        let source = r#"
int main() {
    struct pollfd fds[2];
    poll(fds, 2, timeout);
    return 0;
}
"#;
        let matches = analyzer.analyze(source);
        assert!(matches.iter().any(|m| matches!(m.pattern, PosixPattern::Poll)));
        let poll_match = matches.iter().find(|m| matches!(m.pattern, PosixPattern::Poll)).unwrap();
        assert!(matches!(poll_match.transformability, crate::Transformability::NotTransformable));
    }

    #[test]
    fn test_detect_file_operations() {
        let mut analyzer = CAnalyzer::new();
        let source = r#"
int main() {
    FILE* f = fopen("test.txt", "r");
    fread(buf, 1, size, f);
    fwrite(buf, 1, size, f);
    fclose(f);
    return 0;
}
"#;
        let matches = analyzer.analyze(source);
        let patterns: Vec<_> = matches.iter().map(|m| m.pattern.clone()).collect();
        assert!(patterns.contains(&PosixPattern::Fopen));
        assert!(patterns.contains(&PosixPattern::Fread));
        assert!(patterns.contains(&PosixPattern::Fwrite));
        assert!(patterns.contains(&PosixPattern::Fclose));
    }
}
