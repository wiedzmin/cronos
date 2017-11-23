package cronos

// TODO: write tests
// TODO: improve logging
// TODO: add inline docs (header)
// TODO: add license
// TODO: add readme
// TODO: maybe add examples

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// CronJob ... local cron job definition
type CronJob struct {
	// job definitions
	Command     string `yaml:"command"`
	Description string `yaml:"description"`
	Pwd         string `yaml:"pwd"`
	// time definitions
	Minute  *int          `yaml:"minute"`
	Hour    *int          `yaml:"hour"`
	Day     *int          `yaml:"day"`
	Weekday *time.Weekday `yaml:"weekday"`
	Month   *time.Month   `yaml:"month"`
	// is it "timestamp" or recurring
	Every bool `yaml:"every"`
	// misc
	Timeout *int `yaml:"timeout"`
	Lock    bool `yaml:"lock"`
	// control timestamps
	lastRun *time.Time
	nextRun *time.Time
	// internal
	m sync.Mutex
	x bool
}

// CronJobs ... Scheduled local jobs definitions
type CronJobs struct {
	Jobs []CronJob `yaml:"crontab"`
}

// Scheduler ... cron jobs holder
type Scheduler struct {
	Jobs []CronJob
	log  *zap.SugaredLogger
	Size int
}

// ValidateCrontab ... check if job definition is minimally correct
func validateCrontab(crontab *CronJobs) ([]CronJob, error) {
	jobs := crontab.Jobs
	if len(jobs) == 0 {
		return nil, errors.New("Empty jobs list")
	}
	var validJobs []CronJob
	var jobErrors []string
	pwd, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		panic(err)
	}
	for i, job := range jobs {
		var valid = true
		if job.Command == "" {
			valid = false
			jobErrors = append(jobErrors, fmt.Sprintf("\njob #%d: no command provided", i))
		}
		if job.Description == "" {
			valid = false
			jobErrors = append(jobErrors, fmt.Sprintf("\njob #%d: no description provided", i))
		}
		if job.Every != true {
			valid = false
			jobErrors = append(jobErrors, fmt.Sprintf("\njob #%d: scheduling without \"every\" keyword is not implemented yet", i))
		} else {
			timeDefsFound := 0
			if job.Minute != nil {
				timeDefsFound++
			}
			if job.Hour != nil {
				timeDefsFound++
			}
			if job.Day != nil {
				timeDefsFound++
			}
			if job.Weekday != nil {
				timeDefsFound++
			}
			if job.Month != nil {
				timeDefsFound++
			}
			if timeDefsFound > 1 {
				valid = false
				jobErrors = append(jobErrors, fmt.Sprintf("\njob #%d: with \"every\" set to \"true\" exactly one time definition (Minute|Hour|Day|Weekday|Month) could be specified", i))
			}
		}
		if job.Pwd == "" {
			job.Pwd = pwd
		}
		if valid == true {
			validJobs = append(validJobs, job)
		}
	}
	if len(jobErrors) != 0 {
		var errBuf bytes.Buffer
		for _, err := range jobErrors {
			errBuf.WriteString(err)
		}
		return validJobs, errors.New(errBuf.String())
	}
	return validJobs, nil
}

// LoadCrontab ... load cron jobs definitions
func LoadCrontab(filename string) ([]CronJob, error) {
	var ctab CronJobs
	filenameAbs, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}
	contents, err := ioutil.ReadFile(filenameAbs)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal([]byte(contents), &ctab)
	if err != nil {
		return nil, err
	}
	return validateCrontab(&ctab)
}

// InitLogging ... Initialize loggers
func InitLogging(logFilename string, debug bool, showLoc bool) (*zap.Logger, *zap.SugaredLogger) {
	var rawlog *zap.Logger
	var log *zap.SugaredLogger
	var cfg zap.Config
	var err error
	if debug {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	cfg.OutputPaths = []string{"stdout", logFilename}
	cfg.DisableCaller = !showLoc
	rawlog, err = cfg.Build()
	if err != nil {
		panic(err)
	}
	log = rawlog.Sugar()
	return rawlog, log
}

// Run ... Run task
func (j *CronJob) Run(log *zap.SugaredLogger) {
	log.Debugw("considering", "job", j)
	if !j.Lock {
		go j.doRun(log)
		j.reschedule()
		return
	}
	j.m.Lock()
	defer j.m.Unlock()
	if j.x {
		log.Debugw("already running", "job", j)
		return
	}
	j.x = true
	go func() {
		defer func() {
			j.m.Lock()
			defer j.m.Unlock()
			j.x = false
		}()
		j.doRun(log)
	}()
}

func (j *CronJob) doRun(log *zap.SugaredLogger) {
	log.Infow("running", "job", j)
	var cmd *exec.Cmd
	if j.Timeout == nil {
		cmd = exec.Command("/bin/sh", "-c", j.Command)
	} else {
		timeout := time.Second * time.Duration(*j.Timeout)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cmd = exec.CommandContext(ctx, "/bin/sh", "-c", j.Command)
	}
	cmd.Dir = j.Pwd
	wp, err := cmd.StdinPipe()
	if err != nil {
		log.Fatalw("stdinpipe", "error", err)
	}
	wp.Close()
	out, err := cmd.CombinedOutput()
	strOut := string(out)
	log.Infow("completed", "job", j, "out", strOut, "err", err)
	if err != nil {
		panic(err) // FIXME
	}
}

// WeekdayToDayRelative ... guess day from weekday, either before or after current [week]day
func WeekdayToDayRelative(weekday int, forward bool) int {
	nowts := time.Now()
	diff := int(nowts.Weekday()) - weekday
	if diff < 0 {
		diff = 7 + diff
	}
	result := nowts.Add(-time.Duration(diff) * time.Minute * 60 * 24)
	if forward == true {
		result = result.Add(7 * time.Minute * 60 * 24)
	}
	return result.Day()
}

func (j *CronJob) reschedule() {
	nowtime := time.Now()
	if j.lastRun == nil {
		if j.Weekday != nil {
			adjustedNow := time.Date(time.Now().Year(), time.Now().Month(), WeekdayToDayRelative(int(*j.Weekday), false), 0, 0, 0, 0, time.Local)
			j.lastRun = &adjustedNow
		} else {
			j.lastRun = &nowtime
		}
	} else {
		*j.lastRun = *j.nextRun
	}
	if j.Every == true {
		var closest time.Time
		var period time.Duration
		if j.Minute != nil {
			period = time.Duration(time.Duration(*j.Minute) * time.Minute)
		} else if j.Hour != nil {
			closest = time.Date(nowtime.Year(), nowtime.Month(), nowtime.Day(), nowtime.Hour()+int(*j.Hour), 0, 0, 0, time.Local)
			period = time.Duration(closest.Sub(nowtime))
		} else if j.Day != nil {
			closest = time.Date(nowtime.Year(), nowtime.Month(), nowtime.Day()+int(*j.Day), 0, 0, 0, 0, time.Local)
			period = time.Duration(closest.Sub(nowtime))
		} else if j.Weekday != nil {
			closest = time.Date(nowtime.Year(), nowtime.Month(), WeekdayToDayRelative(int(*j.Weekday), true), 0, 0, 0, 0, time.Local)
			period = time.Duration(closest.Sub(nowtime))
		} else if j.Month != nil {
			closest = time.Date(nowtime.Year(), nowtime.Month()+time.Month(*j.Month), 0, 0, 0, 0, 0, time.Local)
			period = time.Duration(closest.Sub(nowtime))
		}
		nextRun := j.lastRun.Add(period)
		j.nextRun = &nextRun
	} else {
		// TODO: implement
	}
}

// NewScheduler ... create new scheduler
func NewScheduler(log *zap.SugaredLogger) *Scheduler {
	return &Scheduler{[]CronJob{}, log, 0}
}

// AddJob ... add job to scheduler
func (s *Scheduler) AddJob(job CronJob) {
	s.Jobs = append(s.Jobs, job)
	s.Jobs[s.Size].reschedule()
}

// AddJobs ... add nultiple jobs to scheduler
func (s *Scheduler) AddJobs(jobs []CronJob) {
	start := s.Size
	s.Jobs = append(s.Jobs, jobs...)
	s.Size += len(jobs)
	finish := s.Size
	for i := start; i < finish; i++ {
		s.Jobs[i].reschedule()
	}
}

// TODO: implement scheduler cleanup (jobs removal)

// RunPending ... run jobs pending at the moment
func (s *Scheduler) RunPending() {
	for i := 0; i < s.Size; i++ {
		if time.Now().After(*s.Jobs[i].nextRun) {
			s.Jobs[i].Run(s.log)
		}
	}
}

// Start ... start scheduler
func (s *Scheduler) Start() chan bool {
	stopped := make(chan bool, 1)
	ticker := time.NewTicker(1 * time.Minute)

	go func() {
		for {
			select {
			case <-ticker.C:
				s.RunPending()
			case <-stopped:
				return
			}
		}
	}()

	return stopped
}
