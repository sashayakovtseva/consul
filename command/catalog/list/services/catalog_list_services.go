package services

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/command/flags"
	"github.com/mitchellh/cli"
	"github.com/ryanuber/columnize"
)

func New(ui cli.Ui) *cmd {
	c := &cmd{UI: ui}
	c.init()
	return c
}

type cmd struct {
	UI    cli.Ui
	flags *flag.FlagSet
	http  *flags.HTTPFlags
	help  string

	// flags
	node        string
	nodeMeta    map[string]string
	tags        []string
	serviceName string
}

func (c *cmd) init() {
	c.flags = flag.NewFlagSet("", flag.ContinueOnError)
	c.flags.StringVar(&c.node, "node", "",
		"Node `id or name` for which to list services.")
	c.flags.Var((*flags.FlagMapValue)(&c.nodeMeta), "node-meta", "Metadata to "+
		"filter nodes with the given `key=value` pairs. If specified, only "+
		"services running on nodes matching the given metadata will be returned. "+
		"This flag may be specified multiple times to filter on multiple sources "+
		"of metadata.")
	c.flags.StringVar(&c.serviceName, "service", "",
		"Service `id or name` to list entries.")
	c.flags.Var((*flags.CommaSliceValue)(&c.tags), "tags", "Tags to filter services.")

	c.http = &flags.HTTPFlags{}
	flags.Merge(c.flags, c.http.ClientFlags())
	flags.Merge(c.flags, c.http.ServerFlags())
	flags.Merge(c.flags, c.http.MultiTenancyFlags())
	c.help = flags.Usage(help, c.flags)
}

func (c *cmd) Run(args []string) int {
	if err := c.flags.Parse(args); err != nil {
		return 1
	}

	if l := len(c.flags.Args()); l > 0 {
		c.UI.Error(fmt.Sprintf("Too many arguments (expected 0, got %d)", l))
		return 1
	}

	// Create and test the HTTP client
	client, err := c.http.APIClient()
	if err != nil {
		c.UI.Error(fmt.Sprintf("Error connecting to Consul agent: %s", err))
		return 1
	}

	queryOptions := &api.QueryOptions{
		NodeMeta: c.nodeMeta,
	}

	var services map[string][]service

	if c.node != "" {
		catalogNode, _, err := client.Catalog().Node(c.node, queryOptions)
		if err != nil {
			c.UI.Error(fmt.Sprintf("Error listing services for node: %s", err))
			return 1
		}

		if catalogNode != nil {
			services = make(map[string][]service, len(catalogNode.Services))
			for _, s := range catalogNode.Services {
				if c.serviceName != "" && s.Service != c.serviceName {
					continue
				}

				if isSubset(s.Tags, c.tags) {
					services[s.Service] = append(services[s.Service], service{
						addr: s.Address,
						id:   s.ID,
						node: catalogNode.Node.Node,
						tags: s.Tags,
					})
				}
			}
		}
	} else {
		catalogServices, _, err := client.Catalog().Services(queryOptions)
		if err != nil {
			c.UI.Error(fmt.Sprintf("Error listing services: %s", err))
			return 1
		}

		if len(catalogServices) > 0 {
			services = make(map[string][]service, len(catalogServices))
			for serviceName := range catalogServices {
				if c.serviceName != "" && serviceName != c.serviceName {
					continue
				}

				svcs, _, err := client.Catalog().Service(serviceName, "", queryOptions)
				if err != nil {
					c.UI.Error(fmt.Sprintf("Error listing services for %s: %s", serviceName, err))
					return 1
				}

				for _, svc := range svcs {
					if isSubset(svc.ServiceTags, c.tags) {
						services[serviceName] = append(services[serviceName], service{
							addr: svc.Address,
							id:   svc.ID,
							node: svc.Node,
							tags: svc.ServiceTags,
						})
					}
				}
			}
		}
	}

	// Handle the edge case where there are no services that match the query.
	if len(services) == 0 {
		c.UI.Error("No services match the given query - try expanding your search.")
		return 0
	}

	c.UI.Info(printServices(services))
	return 0
}

func (c *cmd) Synopsis() string {
	return synopsis
}

func (c *cmd) Help() string {
	return c.help
}

const (
	synopsis = "Lists all registered services in a datacenter"
	help     = `
Usage: consul catalog services [options]

  Retrieves the list services registered in a given datacenter. By default, the
  datacenter of the local agent is queried.

  To retrieve the list of services:

      $ consul catalog services

  To include the services' tags in the output:

      $ consul catalog services -tags

  To list services which run on a particular node:

      $ consul catalog services -node=web

  To filter services on node metadata:

      $ consul catalog services -node-meta="foo=bar"

  For a full list of options and examples, please see the Consul documentation.
`
)

type service struct {
	addr string
	id   string
	node string
	tags []string
}

type byAddr []service

func (b byAddr) Len() int {
	return len(b)
}

func (b byAddr) Less(i, j int) bool {
	return b[i].addr < b[j].addr
}

func (b byAddr) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func isSubset(set, subSet []string) bool {
	if len(subSet) == 0 {
		return true
	}

loop:
	for _, subElement := range subSet {
		for _, element := range set {
			if element == subElement {
				continue loop
			}
		}
		return false
	}

	return true
}

func printServices(services map[string][]service) string {
	// Order the map for consistent output
	order := make([]string, 0, len(services))
	for k := range services {
		order = append(order, k)
	}
	sort.Strings(order)

	result := make([]string, 0, len(services)+1)
	header := "Service\x1fID\x1fAddress\x1fNode\x1fTags"
	result = append(result, header)

	for _, serviceName := range order {
		sort.Sort(byAddr(services[serviceName]))

		for _, svc := range services[serviceName] {
			result = append(result, fmt.Sprintf("%s\x1f%s\x1f%s\x1f%s\x1f%s",
				serviceName,
				svc.id,
				svc.addr,
				svc.node,
				strings.Join(svc.tags, ", "),
			))
		}

	}

	return columnize.Format(result, &columnize.Config{Delim: string([]byte{0x1f})})
}
