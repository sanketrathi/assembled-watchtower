package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"watchtower/internal/runtime"
	"watchtower/internal/storage"
	"watchtower/internal/web"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "migrate":
		err = migrate(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:])
	case "replay":
		err = replay(os.Args[2:])
	case "help", "--help", "-h":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func usage() {
	fmt.Println("watchtower [migrate|serve|replay]\n\nCommands:\n  migrate  apply (or explicitly reset) the PostgreSQL schema\n  serve    run the team-lead dashboard and JSON read endpoints\n  replay   replay JSONL events into alerts and visible stub notifications")
}

func commonFlags(fs *flag.FlagSet) (*string, *int) {
	envURL := os.Getenv("WATCHTOWER_DATABASE_URL")
	envConns := 8
	if raw := os.Getenv("WATCHTOWER_DB_MAX_CONNS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			envConns = -1
		} else {
			envConns = n
		}
	}
	return fs.String("database-url", envURL, "PostgreSQL connection URL"), fs.Int("db-max-conns", envConns, "maximum PostgreSQL connections")
}
func open(ctx context.Context, url string, conns int) (*storage.DB, error) {
	if strings.TrimSpace(url) == "" {
		return nil, errors.New("database URL is required (set WATCHTOWER_DATABASE_URL or --database-url)")
	}
	if conns < 1 {
		return nil, errors.New("db-max-conns must be a positive integer")
	}
	return storage.Open(ctx, url, int32(conns))
}
func migrate(args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	url, conns := commonFlags(fs)
	reset := fs.Bool("reset", false, "destructively reset the public schema (local use only)")
	yes := fs.Bool("yes", false, "confirm a destructive reset")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	ctx := context.Background()
	db, err := open(ctx, *url, *conns)
	if err != nil {
		return err
	}
	defer db.Close()
	if *reset {
		if !*yes {
			return errors.New("--reset requires --yes")
		}
		if err = storage.Reset(ctx, db.Pool); err != nil {
			return err
		}
	} else if err = storage.ApplyMigrations(ctx, db.Pool); err != nil {
		return err
	}
	fmt.Println("database schema is up to date")
	return nil
}
func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	url, conns := commonFlags(fs)
	addr := fs.String("listen-addr", valueOr("WATCHTOWER_HTTP_ADDR", ":8080"), "HTTP listen address")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	db, err := open(ctx, *url, *conns)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = storage.ApplyMigrations(ctx, db.Pool); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Pool.Ping(r.Context()); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/api/", http.StripPrefix("/api", runtime.API(db.Pool)))
	dashboard := web.NewHandler(runtime.Site{Pool: db.Pool})
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/rules") {
			saveDemoRule(db.Pool, w, r)
			return
		}
		dashboard.ServeHTTP(w, r)
	}))
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("watchtower listening on %s", *addr)
	err = srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func replay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	url, conns := commonFlags(fs)
	input := fs.String("input", valueOr("WATCHTOWER_REPLAY_FILE", "data/events.jsonl"), "JSONL input file")
	rulesPath := fs.String("rules", valueOr("WATCHTOWER_RULES_FILE", "data/demo-rules.json"), "active rule definitions")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	ctx := context.Background()
	db, err := open(ctx, *url, *conns)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = storage.ApplyMigrations(ctx, db.Pool); err != nil {
		return err
	}
	summary, err := runtime.Run(ctx, db.Pool, runtime.Config{EventPath: *input, RulesPath: *rulesPath, Source: "demo-replay", StreamID: *input})
	if err != nil {
		return err
	}
	fmt.Printf("replay complete: %d events, %d alert transitions, %d visible notifications\n", summary.Occurrences, summary.Alerts, summary.Notifications)
	return nil
}

func saveDemoRule(pool *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "read rule form", http.StatusBadRequest)
		return
	}
	template := web.TemplateKind(r.FormValue("template_choice"))
	if template == "" {
		template = web.TemplateKind(r.FormValue("template"))
	}
	definition, err := runtime.Definition(template, r.FormValue("name"), r.FormValue("description"), r.FormValue("target_ids"), r.FormValue("trigger_for"), r.FormValue("audience"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	if r.FormValue("intent") == "preview" {
		f, openErr := os.Open(valueOr("WATCHTOWER_REPLAY_FILE", "data/events.jsonl"))
		if openErr != nil {
			http.Error(w, "open preview events: "+openErr.Error(), http.StatusInternalServerError)
			return
		}
		defer f.Close()
		items, previewErr := runtime.Replay(f, []runtime.Rule{{ID: "preview", Revision: 1, Definition: definition}})
		if previewErr != nil {
			http.Error(w, "preview rule: "+previewErr.Error(), http.StatusUnprocessableEntity)
			return
		}
		preview := &web.PreviewView{Status: "ready", Summary: fmt.Sprintf("This rule would create %d notifications from the supplied morning.", len(items))}
		for _, item := range items {
			preview.Alerts = append(preview.Alerts, web.PreviewAlert{Subject: item.SubjectKind + ":" + item.SubjectID, Outcome: item.Alert.Kind.String(), At: item.Alert.At.Format(time.RFC3339), Explanation: "Visible stub notification for " + item.Audience})
		}
		view := web.RuleFormView{Mode: "create", Title: "Preview this rule", Action: "/rules", CancelURL: "/", SelectedTemplate: template, Templates: web.StandardTemplates(), Values: web.RuleFormValues{Name: r.FormValue("name"), Description: r.FormValue("description"), TargetIDs: r.FormValue("target_ids"), TriggerFor: r.FormValue("trigger_for"), ClearFor: "0s", Audience: r.FormValue("audience"), NotifyOnOpen: true, NotifyOnRecovery: true}, Preview: preview}
		if err := web.RenderRuleForm(w, view); err != nil {
			http.Error(w, "render preview", 500)
		}
		return
	}
	id, err := runtime.SaveRule(r.Context(), pool, r.FormValue("rule_id"), definition)
	if err != nil {
		http.Error(w, "save rule: "+err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/?saved="+id, http.StatusSeeOther)
}

func valueOr(k, f string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return f
}
