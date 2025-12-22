package repositories

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"service-info/internal/models"
)

type AdminRepository struct {
	db *sql.DB
}

func NewAdminRepository(db *sql.DB) *AdminRepository {
	return &AdminRepository{db: db}
}

func (r *AdminRepository) DB() *sql.DB {
	return r.db
}

func (r *AdminRepository) Save(ctx context.Context, task models.Task) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO scheduled_tasks (title, args, created_at)
		VALUES ($1, $2, $3)
	`, task.Title, task.Args, task.CreatedAt)
	return err
}

func (r *AdminRepository) GetTopRequests(ctx context.Context) ([]models.PopularRequest, error) {
	const sqlQuery = `
SELECT * FROM (
    SELECT 'weather' AS type,
           jsonb_build_object('city', args->>'city') AS args,
           COUNT(*) AS cnt
    FROM scheduled_tasks
    WHERE title = 'weather' 
      AND created_at >= NOW() - INTERVAL '24 hours'
      AND args ? 'city'
    GROUP BY args->>'city'
    ORDER BY cnt DESC
    LIMIT 5
) AS weather_top
UNION ALL
SELECT * FROM (
    SELECT 'exchange' AS type, 
           jsonb_build_object('base', args->>'base', 'target', args->>'target') AS args,
           COUNT(*) AS cnt
    FROM scheduled_tasks
    WHERE title = 'exchange'
      AND created_at >= NOW() - INTERVAL '24 hours'
      AND args ? 'base' AND args ? 'target'
    GROUP BY args->>'base', args->>'target'
    ORDER BY cnt DESC
    LIMIT 5
) AS exchange_top;
`

	rows, err := r.db.QueryContext(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var topRequests []models.PopularRequest
	for rows.Next() {
		var typ string
		var argsJSON []byte
		var cnt int

		if err := rows.Scan(&typ, &argsJSON, &cnt); err != nil {
			return nil, err
		}

		var args models.TaskArgs
		if err := json.Unmarshal(argsJSON, &args); err != nil {
			log.Printf("Unmarshal args failed: %v", err)
			continue
		}

		topRequests = append(topRequests, models.PopularRequest{
			Type: typ,
			Args: args,
		})
	}

	return topRequests, rows.Err()
}
