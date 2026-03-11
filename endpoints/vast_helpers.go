package endpoints

// emptyVAST returns a minimal, well-formed VAST 3.0 document with no Ad elements.
// Use open/close tags (not self-closing) for broader player compatibility.
func emptyVAST() string {
	return `<?xml version="1.0" encoding="UTF-8"?><VAST version="3.0"></VAST>`
}
