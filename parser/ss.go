// Parse Sidestream filename like 20170516T22:00:00Z_163.7.129.73_0.web100
package parser

import (
	"bufio"
	"cloud.google.com/go/bigquery"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/schema"
)

type SSParser struct {
	inserter etl.Inserter
}

func NewSSParser(ins etl.Inserter) *SSParser {
	return &SSParser{ins}
}


func ParseSSFilename(testName string) {
}

func ParseIPFamily(ipStr string) int {
  ip := net.ParseIP(ipStr)
  if ip.To4() != nil {
    return syscall.AF_INET
  } else if ip.To16() != nil {
      return syscall.AF_INET6)
  }
  return -1
}

// the first line of SS test is in format "K: web100_variables_separated_by_space"
func ParseHeader(header string) (web100_var []string, error) {
  web100_vars := strings.Split(header, " ")
  if web100_vars[0] != "K:" {
    
  }
  return web100_vars[1:]
}

func ParseOneLine(snapshot string) error {
  value := strings.Split(snapshot, " ")
  if value[0] != "C:" {
    return 
  }
  
}

func (ss *SSParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
   time := ParseSSFilename(testName)
}
