"""Tests for export_cluster_run (Phase 17)."""

import csv
import io
import json

import pytest
from typer.testing import CliRunner

from fluster.cli import app
from fluster.config.project import project_dir
from fluster.db.connection import connect
from fluster.pipeline.export import EXPORT_HEADERS, export_cluster_run

runner = CliRunner()


def _seed_cluster_run(conn, n_items=5, with_labels=True, with_umap2=True):
    """Seed rows, items, assignments, and optionally labels + UMAP-2D coords.

    Returns the cluster_run_id.
    """
    # Create rows and items.
    for i in range(n_items):
        conn.execute(
            "INSERT INTO rows (row_name, row_metadata_json) VALUES (?, ?)",
            (f"row_{i}", json.dumps({"index": i})),
        )
        row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute("INSERT INTO items (row_id) VALUES (?)", (row_id,))

    # Create a reduction and cluster run.
    conn.execute(
        "INSERT INTO reductions (embedding_reference, method, target_dimensions) "
        "VALUES ('test', 'umap', 8)"
    )
    red8_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        "INSERT INTO cluster_runs (reduction_id, method, params_json) "
        "VALUES (?, 'hdbscan', '{}')",
        (red8_id,),
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Assign items to clusters (alternate 0 and 1, last item is noise = -1).
    items = conn.execute("SELECT item_id FROM items ORDER BY item_id").fetchall()
    for idx, item in enumerate(items):
        cluster_id = -1 if idx == n_items - 1 else idx % 2
        conn.execute(
            "INSERT INTO cluster_assignments (cluster_run_id, item_id, cluster_id, membership_probability) "
            "VALUES (?, ?, ?, 0.9)",
            (run_id, item["item_id"], cluster_id),
        )

    # Optionally add UMAP-2D reduction + coordinates.
    if with_umap2:
        conn.execute(
            "INSERT INTO reductions (embedding_reference, method, target_dimensions) "
            "VALUES ('test', 'umap', 2)"
        )
        red2_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for idx, item in enumerate(items):
            conn.execute(
                "INSERT INTO reduction_coordinates (reduction_id, item_id, coordinates_json) "
                "VALUES (?, ?, ?)",
                (red2_id, item["item_id"], json.dumps([float(idx), float(idx) * -0.5])),
            )

    # Optionally add labels.
    if with_labels:
        for cid in (0, 1):
            label_json = json.dumps({
                "label": f"Cluster {cid}",
                "short_label": f"C{cid}",
                "rationale": "Test.",
                "keywords": ["test"],
            })
            conn.execute(
                "INSERT INTO cluster_summaries (cluster_run_id, cluster_id, label, label_json) "
                "VALUES (?, ?, ?, ?)",
                (run_id, cid, f"Cluster {cid}", label_json),
            )

    conn.commit()
    return run_id


# --- Full export ---


def test_export_full_data(project):
    pdir, conn = project
    run_id = _seed_cluster_run(conn, n_items=5)

    result = export_cluster_run(conn, run_id)

    reader = csv.reader(io.StringIO(result))
    rows = list(reader)

    assert rows[0] == EXPORT_HEADERS
    assert len(rows) == 6  # header + 5 items


def test_export_row_contents(project):
    pdir, conn = project
    run_id = _seed_cluster_run(conn, n_items=3, with_labels=True, with_umap2=True)

    result = export_cluster_run(conn, run_id)
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)

    # First row should have cluster 0 and its label.
    assert rows[0]["row_name"] == "row_0"
    assert rows[0]["cluster_id"] == "0"
    assert rows[0]["cluster_label"] == "Cluster 0"
    assert rows[0]["umap2_x"] != ""
    assert rows[0]["umap2_y"] != ""
    assert "index" in rows[0]["metadata"]


# --- Without labels ---


def test_export_without_labels(project):
    pdir, conn = project
    run_id = _seed_cluster_run(conn, n_items=3, with_labels=False)

    result = export_cluster_run(conn, run_id)
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)

    for row in rows:
        assert row["cluster_label"] == ""


# --- Without UMAP-2D ---


def test_export_without_umap2(project):
    pdir, conn = project
    run_id = _seed_cluster_run(conn, n_items=3, with_umap2=False)

    result = export_cluster_run(conn, run_id)
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)

    for row in rows:
        assert row["umap2_x"] == ""
        assert row["umap2_y"] == ""


# --- Noise items ---


def test_export_includes_noise(project):
    pdir, conn = project
    run_id = _seed_cluster_run(conn, n_items=5)

    result = export_cluster_run(conn, run_id)
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)

    cluster_ids = [row["cluster_id"] for row in rows]
    assert "-1" in cluster_ids


# --- Nonexistent cluster run ---


def test_export_nonexistent_run(project):
    pdir, conn = project
    with pytest.raises(ValueError, match="not found"):
        export_cluster_run(conn, 9999)


# --- Metadata ---


def test_export_metadata(project):
    pdir, conn = project
    run_id = _seed_cluster_run(conn, n_items=2, with_labels=False, with_umap2=False)

    result = export_cluster_run(conn, run_id)
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)

    meta = json.loads(rows[0]["metadata"])
    assert meta["index"] == 0


# --- CLI integration ---


def test_cli_export_stdout(named_project):
    conn = connect(project_dir(named_project))
    run_id = _seed_cluster_run(conn, n_items=3)
    conn.close()

    result = runner.invoke(app, ["export", "test-proj", "--cluster-run", str(run_id)])
    assert result.exit_code == 0
    assert "row_id" in result.output
    assert "row_0" in result.output


def test_cli_export_to_file(named_project, tmp_path):
    conn = connect(project_dir(named_project))
    run_id = _seed_cluster_run(conn, n_items=3)
    conn.close()

    out_file = tmp_path / "export.csv"
    result = runner.invoke(
        app, ["export", "test-proj", "--cluster-run", str(run_id), "--output", str(out_file)]
    )
    assert result.exit_code == 0
    assert out_file.exists()
    contents = out_file.read_text()
    assert "row_id" in contents


def test_cli_export_nonexistent_run(named_project):
    result = runner.invoke(app, ["export", "test-proj", "--cluster-run", "9999"])
    assert result.exit_code == 1


def test_cli_export_nonexistent_project(tmp_path, monkeypatch):
    from fluster.config import settings
    home = tmp_path / ".fluster"
    monkeypatch.setattr(settings, "FLUSTER_HOME", home)
    monkeypatch.setattr(settings, "PROJECTS_DIR", home / "projects")
    result = runner.invoke(app, ["export", "nope", "--cluster-run", "1"])
    assert result.exit_code == 1
