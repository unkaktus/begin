package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"path"
	"strings"
	"text/template"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	BatchPBS         = "pbs"
	BatchSlurm       = "slurm"
	BatchBare        = "bare"
	BatchAutodetect  = "autodetect"
	BatchUnsupported = "unsupported"
)

func DetectBatchSystem() string {
	if _, err := exec.LookPath("qsub"); err == nil {
		return BatchPBS
	}
	if _, err := exec.LookPath("squeue"); err == nil {
		return BatchSlurm
	}
	if _, err := exec.LookPath("bash"); err == nil {
		return BatchBare
	}
	return BatchUnsupported
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

type Config struct {
	Name                         string
	NumberOfNodes                int
	NodeType                     string
	NumberOfMPIRanksPerNode      int
	NumberOfOMPThreadsPerProcess int
	Walltime                     time.Duration
	Email                        string
	LogDirectory                 string
	PrintOMPEnvironment          bool

	ModulesPreScript []string
	LoadModules      []string

	WorkingDirectory string

	PreScript []string

	RunTime    []string
	Executable string
	Arguments  []string

	PostScript []string
}

type ExtendedConfig struct {
	Config
	NumberOfMPIRanks     int
	NumberOfTasksPerNode int
	WalltimeString       string
	OutputFile           string
	ErrorFile            string
}

func NewExtendedConfig(c Config) ExtendedConfig {
	cc := ExtendedConfig{
		Config:               c,
		NumberOfTasksPerNode: 1,
		NumberOfMPIRanks:     c.NumberOfNodes * c.NumberOfMPIRanksPerNode,
		WalltimeString:       formatDuration(c.Walltime),
		OutputFile:           path.Join(c.LogDirectory, c.Name+".out"),
		ErrorFile:            path.Join(c.LogDirectory, c.Name+".err"),
	}
	if c.NumberOfMPIRanksPerNode == 0 {
		cc.NumberOfTasksPerNode = 1
	}
	return cc
}

func ExecTemplate(ts string, s interface{}) (string, error) {
	t, err := template.New("template").Parse(ts)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)

	}
	builder := &strings.Builder{}

	err = t.Execute(builder, s)
	if err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return builder.String(), nil
}

func (config Config) writePBSHeader(builder *strings.Builder) error {
	pbsString, err := ExecTemplate(`#!/bin/bash -l
#PBS -N {{.Name}}
#PBS -e {{.ErrorFile}}
#PBS -o {{.OutputFile}}
#PBS -m abe
#PBS -M {{.Email}}
#PBS -l select={{.NumberOfNodes}}`+
		`:node_type={{.NodeType}}`+
		`:mpiprocs={{.NumberOfMPIRanksPerNode}}`+
		`:ompthreads={{.NumberOfOMPThreadsPerProcess}}`+`
#PBS -l walltime={{.WalltimeString}}
`,
		NewExtendedConfig(config),
	)
	if err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	builder.WriteString(pbsString)
	builder.WriteString("\n")

	return nil
}

func (config Config) writeSlurmHeader(builder *strings.Builder) error {
	pbsString, err := ExecTemplate(`#!/bin/bash -l
#SBATCH -J {{.Name}}
#SBATCH -o {{.OutputFile}}
#SBATCH -e {{.ErrorFile}}
#SBATCH --mail-type=ALL
#SBATCH --mail-user={{.Email}}
#SBATCH --nodes {{.NumberOfNodes}}
#SBATCH --ntasks-per-node {{.NumberOfTasksPerNode}}
#SBATCH --time={{.WalltimeString}}
`,
		NewExtendedConfig(config),
	)
	if err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	builder.WriteString(pbsString)
	builder.WriteString("\n")

	return nil
}

func (config Config) writeBareHeader(builder *strings.Builder) error {
	pbsString, err := ExecTemplate(`#!/bin/bash -l
`,
		NewExtendedConfig(config),
	)
	if err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	builder.WriteString(pbsString)
	builder.WriteString("\n")

	return nil
}

func (config Config) JobData(batchSystem string) (string, error) {
	builder := &strings.Builder{}

	switch batchSystem {
	case BatchPBS:
		if err := config.writePBSHeader(builder); err != nil {
			return "", fmt.Errorf("write PBS header: %w", err)
		}
	case BatchSlurm:
		if err := config.writeSlurmHeader(builder); err != nil {
			return "", fmt.Errorf("write Slurm header: %w", err)
		}
	case BatchBare:
		if err := config.writeBareHeader(builder); err != nil {
			return "", fmt.Errorf("write bare header: %w", err)
		}
	}

	for _, cmd := range config.ModulesPreScript {
		builder.WriteString(cmd + "\n")
	}
	builder.WriteString("\n")

	for _, module := range config.LoadModules {
		builder.WriteString(fmt.Sprintf("module load %s\n", module))
	}
	builder.WriteString("\n")

	for _, cmd := range config.PreScript {
		builder.WriteString(cmd + "\n")
	}
	builder.WriteString("\n")

	if config.WorkingDirectory != "" {
		builder.WriteString("cd " + config.WorkingDirectory + "\n")
	}
	builder.WriteString("\n")

	task := []string{}

	if config.NumberOfMPIRanksPerNode > 0 {
		mpirunString, err := ExecTemplate("time mpirun -n {{.NumberOfMPIRanks}}",
			NewExtendedConfig(config),
		)
		if err != nil {
			return "", fmt.Errorf("create mpirun string: %w", err)
		}
		if mpirunString != "" {
			task = append(task, mpirunString)
		}
	}

	if len(config.RunTime) > 0 {
		task = append(task, strings.Join(config.RunTime, " "))
	}

	task = append(task, config.Executable)

	if len(config.Arguments) > 0 {
		task = append(task, strings.Join(config.Arguments, " "))
	}

	rawTaskString := strings.Join(task, " ")

	taskStringResult, err := ExecTemplate(rawTaskString, NewExtendedConfig(config))
	if err != nil {
		return "", fmt.Errorf("exec task string template: %w", err)
	}

	builder.WriteString(taskStringResult)
	builder.WriteString("\n")

	for _, cmd := range config.PostScript {
		builder.WriteString(cmd + "\n")
	}

	return builder.String(), nil
}

func run() error {
	batchSystem := flag.String("b", BatchAutodetect, "Batch system to use [pbs, slurm], or default to autodetect")
	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Fatal("Job file is not specified")
	}
	filename := flag.Args()[0]

	config := Config{}

	_, err := toml.DecodeFile(filename, &config)
	if err != nil {
		return fmt.Errorf("decode file: %w", err)
	}

	if *batchSystem == BatchAutodetect {
		*batchSystem = DetectBatchSystem()
	}

	if *batchSystem == BatchUnsupported {
		return fmt.Errorf("unsupported platform")
	}

	jobData, err := config.JobData(*batchSystem)
	if err != nil {
		return fmt.Errorf("getting job data: %w", err)
	}
	fmt.Printf("%s\n", jobData)

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
