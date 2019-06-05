package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/kshvakov/clickhouse"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	yaml "gopkg.in/yaml.v2"
)

type storageCFG struct {
	Addr           string `yaml:"addr"`
	Port           int    `yaml:"port"`
	User           string `yaml:"user"`
	Passwd         string `yaml:"passwd"`
	MaxPings       int    `yaml:"max_pings"`
	DefaultDB      string `yaml:"default_db"`
	WriteTimeoutMS int    `yaml:"write_timeout_ms"`
	ReadTimeoutMS  int    `yaml:"read_timeout_ms"`
	Debug          bool   `json:"debug"`
}

type generatorCFG struct {
	N      int `yaml:"n"`
	InIter int `yaml:"in_iter"`
}

type cfg struct {
	StorageCFG   storageCFG   `yaml:"storage"`
	GeneratorCFG generatorCFG `yaml:"generator"`
}

func readCFG() (*cfg, error) {
	configPath := ""
	flag.StringVar(&configPath, "config", "", "path to YAML configuration file")
	flag.Parse()

	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read configuration file")
	}

	cfg := &cfg{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, errors.Wrap(err, "unable to parse configuration file")
	}

	return cfg, nil
}

type controlObject struct {
	// Special DB fields.
	id   string
	dbts *time.Time
	ts   time.Time
	// Business-Logic fields.
	passport   string
	surname    string
	name       string
	patronymic string
	sex        string
	birthDate  string
	phoneNum   string
	email      string
	address    string
}

const insertControlObjectsQuery = `
INSERT INTO
    control_objects
    (id, ts, passport,
     surname, name, patronymic,
     sex, birthdate,
     phone_num, email, address)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`

func insertControlObjects(db *sql.DB, cobs []controlObject) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "unable to begin bulk insert")
	}
	stmt, err := tx.Prepare(insertControlObjectsQuery)
	if err != nil {
		return errors.Wrap(err, "unable to prepare SQL-statement")
	}
	defer stmt.Close()

	for i, cob := range cobs {
		if _, err := stmt.Exec(
			clickhouse.UUID(cob.id),
			cob.ts,
			cob.passport,
			cob.surname,
			cob.name,
			cob.patronymic,
			cob.sex,
			cob.birthDate,
			cob.phoneNum,
			cob.email,
			cob.address,
		); err != nil {
			return errors.Wrapf(err, "unable to execute %d-th part of bulk insert", i+1)
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "unable to commit bulk insert")
	}

	return nil
}

type ffv struct {
	id                   string
	cobID                string
	imgID                string
	faceBox              []uint64
	facialFeaturesVector []float64
}

const insertFFVsQuery = `
INSERT INTO
    facial_features
    (id, cob_id, img_id, fb, ff)
VALUES
    (?, ?, ?, ?, ?);
`

func insertFFVs(db *sql.DB, ffvs []ffv) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "unable to begin bulk write transaction")
	}
	stmt, err := tx.Prepare(insertFFVsQuery)
	if err != nil {
		return errors.Wrap(err, "unable to prepare InsertFFQuery statemet")
	}
	defer stmt.Close()
	for _, ffv := range ffvs {
		if _, err := stmt.Exec(
			clickhouse.UUID(ffv.id),
			clickhouse.UUID(ffv.cobID),
			clickhouse.UUID(ffv.imgID),
			clickhouse.Array(ffv.faceBox),
			clickhouse.Array(ffv.facialFeaturesVector),
		); err != nil {
			return errors.Wrap(err, "unable to execute part of bulk write transaction. Rollbacking")
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "unable to commit bulk write transaction. Rollbacking")
	}

	return nil
}

func generatePassport() string {
	passport := ""
	for i := 0; i < 12; i++ {
		if (i == 2) || (i == 5) {
			passport += " "
		} else {
			passport += strconv.Itoa(rand.Int() % 10)
		}
	}
	return passport
}

func generateFaceBox() []uint64 {
	faceBox := make([]uint64, 4)
	for i := 0; i < len(faceBox); i++ {
		faceBox[i] = rand.Uint64()
	}
	return faceBox
}

func generateFFV() []float64 {
	ffv := make([]float64, 128)
	for i := 0; i < len(ffv); i++ {
		ffv[i] = rand.Float64()*2.0 - 1.0
	}
	return ffv
}

func main() {
	startTime := time.Now()
	rand.Seed(startTime.Unix())

	cfg, err := readCFG()
	if err != nil {
		fmt.Println(errors.Wrap(err, "unable to read configuration file"))
	}

	connStr := fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&database=%s&read_timeout=%d&write_timeout=%d&debug=%v",
		cfg.StorageCFG.Addr,
		cfg.StorageCFG.Port,
		cfg.StorageCFG.User,
		cfg.StorageCFG.Passwd,
		cfg.StorageCFG.DefaultDB,
		cfg.StorageCFG.ReadTimeoutMS/1000,
		cfg.StorageCFG.WriteTimeoutMS/1000,
		cfg.StorageCFG.Debug)
	db, err := sql.Open("clickhouse", connStr)
	if err != nil {
		fmt.Print(errors.Wrap(err, "unable to connect to ClickHouse"))
		os.Exit(1)
	}
	defer db.Close()
	pingTimes := 0
	for pingTimes = 0; pingTimes < cfg.StorageCFG.MaxPings; pingTimes++ {
		err := db.Ping()
		if err == nil {
			break
		}
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("ClickHouse DB exception: [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(errors.Wrapf(err, "unable to ping ClickHouse DB for %d time", pingTimes+1))
		}
	}
	if pingTimes == cfg.StorageCFG.MaxPings {
		fmt.Println(fmt.Errorf("unable to ping ClickHouse DB for %d times", cfg.StorageCFG.MaxPings))
		os.Exit(1)
	}

	iters := cfg.GeneratorCFG.N / cfg.GeneratorCFG.InIter
	afterIters := cfg.GeneratorCFG.N % cfg.GeneratorCFG.InIter

	for i := 0; i < iters; i++ {
		cobs := make([]controlObject, cfg.GeneratorCFG.InIter)
		for i := 0; i < len(cobs); i++ {
			cobs[i] = controlObject{
				id:         uuid.Must(uuid.NewV4()).String(),
				ts:         time.Now(),
				passport:   generatePassport(),
				surname:    "-",
				name:       "-",
				patronymic: "-",
				sex:        "-",
				birthDate:  "-",
				phoneNum:   "-",
				email:      "-",
				address:    "-",
			}
		}
		if err := insertControlObjects(db, cobs); err != nil {
			fmt.Println(errors.Wrap(err, "unable to insert generated control objects"))
			os.Exit(1)
		}
		ffvs := make([]ffv, cfg.GeneratorCFG.InIter)
		for i := 0; i < len(ffvs); i++ {
			ffvs[i] = ffv{
				id:                   uuid.Must(uuid.NewV4()).String(),
				cobID:                cobs[i].id,
				imgID:                "00000000-0000-0000-0000-000000000000",
				faceBox:              generateFaceBox(),
				facialFeaturesVector: generateFFV(),
			}
		}
		if err := insertFFVs(db, ffvs); err != nil {
			fmt.Println(errors.Wrap(err, "unable to insert generated facial features vectors"))
			os.Exit(1)
		}
	}

	if afterIters != 0 {
		cobs := make([]controlObject, afterIters)
		for i := 0; i < len(cobs); i++ {
			cobs[i] = controlObject{
				id:         uuid.Must(uuid.NewV4()).String(),
				ts:         time.Now(),
				passport:   generatePassport(),
				surname:    "-",
				name:       "-",
				patronymic: "-",
				sex:        "-",
				birthDate:  "-",
				phoneNum:   "-",
				email:      "-",
				address:    "-",
			}
		}
		if err := insertControlObjects(db, cobs); err != nil {
			fmt.Println(fmt.Errorf("unable to insert generated control objects"))
			os.Exit(1)
		}
		ffvs := make([]ffv, afterIters)
		for i := 0; i < len(ffvs); i++ {
			ffvs[i] = ffv{
				id:                   uuid.Must(uuid.NewV4()).String(),
				cobID:                cobs[i].id,
				imgID:                "00000000-0000-0000-0000-000000000000",
				faceBox:              generateFaceBox(),
				facialFeaturesVector: generateFFV(),
			}
		}
		if err := insertFFVs(db, ffvs); err != nil {
			fmt.Println(fmt.Errorf("unable to insert generated facial features vectors"))
			os.Exit(1)
		}
	}

	fmt.Printf("inserted %d (%d in req) pairs (ControlObject x FacialFeaturesVector) to ClickHouse DB in %v\n",
		cfg.GeneratorCFG.N, cfg.GeneratorCFG.InIter, time.Now().Sub(startTime))
}
