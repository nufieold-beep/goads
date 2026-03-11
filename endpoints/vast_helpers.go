package endpoints

import "strconv"

const emptyVASTString = `<?xml version="1.0" encoding="UTF-8"?><VAST version="3.0"></VAST>`

// Pre-computed empty VAST response bytes to avoid per-request allocation.
var emptyVASTBytes = []byte(emptyVASTString)
var emptyVASTLenStr = strconv.Itoa(len(emptyVASTBytes))

// emptyVAST returns a minimal, well-formed VAST 3.0 document with no Ad elements.
func emptyVAST() string {
	return emptyVASTString
}
