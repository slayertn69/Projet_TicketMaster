from flask import Flask, render_template
import pandas as pd
import numpy as np
import os

# Chemin par défaut vers ton fichier (écrasable via LICHESS_XLSX_PATH)
EXCEL_PATH = os.environ.get(
    "LICHESS_XLSX_PATH",
    "/Users/nass/Desktop/lichess_flask_app/lichess_powerbi - Copie.xlsx"
)

app = Flask(__name__)

def load_games():
    # --- ouvrir le classeur ---
    xls = pd.ExcelFile(EXCEL_PATH)

    # --- trouver les bons onglets ---
    raw_sheet = None
    for name in xls.sheet_names:
        if name.strip().lower() in ("raw_data", "raw-data", "rawdata"):
            raw_sheet = name
            break
    if raw_sheet is None:
        raw_sheet = xls.sheet_names[0]

    gs_sheet = None
    for name in xls.sheet_names:
        if name.strip().lower() in ("game_stats", "game-stats", "gamestats"):
            gs_sheet = name
            break

    # --- lire les données brutes ---
    df_raw = pd.read_excel(xls, raw_sheet)
    df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]

    # colonnes requises
    for col in ["game_id", "player", "color"]:
        if col not in df_raw.columns:
            raise ValueError(f"Colonne manquante '{col}' dans la feuille '{raw_sheet}'. "
                             f"Colonnes trouvées: {df_raw.columns.tolist()}")

    # normaliser les horloges si présentes (pour la durée)
    for c in ("wc", "bc"):
        if c in df_raw.columns:
            df_raw[c] = pd.to_numeric(df_raw[c], errors="coerce")

    # --- extraire joueur blanc/noir par partie ---
    players = (
        df_raw
        .dropna(subset=["game_id", "color", "player"])
        .groupby(["game_id", "color"])["player"]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0])
        .unstack()
        .rename(columns=lambda c: str(c).strip().lower())
        .reset_index()
    )

    # deviner colonnes white/black et renommer
    white_col = next((c for c in players.columns if str(c).lower().startswith("w")), None)
    black_col = next((c for c in players.columns if str(c).lower().startswith("b")), None)
    players = players.rename(columns={white_col: "white_player", black_col: "black_player"})

    # --- calcul de la durée (approx) ---
    # Heuristique : durée ≈ (wc+bc au début) – (wc+bc à la fin)
    duration = pd.DataFrame({"game_id": df_raw["game_id"].unique()})
    if "wc" in df_raw.columns and "bc" in df_raw.columns:
        df_time = df_raw.copy()
        # ordre des enregistrements par partie (indépendant de l'index global)
        df_time["row_order"] = df_time.groupby("game_id").cumcount()

        tmp = (
            df_time
            .dropna(subset=["wc", "bc"])
            .sort_values(["game_id", "row_order"])
            .groupby("game_id")
            .agg(
                start_wc=("wc", "first"),
                start_bc=("bc", "first"),
                end_wc=("wc", "last"),
                end_bc=("bc", "last"),
            )
            .reset_index()
        )
        tmp["duration_sec"] = (tmp["start_wc"] + tmp["start_bc"]
                               - tmp["end_wc"] - tmp["end_bc"]).clip(lower=0)

        duration = duration.merge(tmp[["game_id", "duration_sec"]], on="game_id", how="left")
    else:
        duration["duration_sec"] = np.nan

    def fmt_seconds(sec):
        if pd.isna(sec):
            return None
        sec = int(round(sec))
        m, s = divmod(sec, 60)
        return f"{m:02d}:{s:02d}"

    duration["duration"] = duration["duration_sec"].apply(fmt_seconds)

    # --- game_stats (statut / gagnant / coups) ---
    if gs_sheet is not None:
        df_gs = pd.read_excel(xls, gs_sheet)
        df_gs.columns = [str(c).strip().lower() for c in df_gs.columns]
    else:
        df_gs = pd.DataFrame({"game_id": df_raw["game_id"].unique()})

    status_col = next((c for c in df_gs.columns if "status" in c), None)
    winner_col = next((c for c in df_gs.columns if "winner" in c), None)
    moves_col  = next((c for c in df_gs.columns if "move"   in c), None)  # ex: "ptal_move"

    # --- normaliser winner pour éviter les NaN/float ---
    if winner_col and winner_col in df_gs.columns:
        def normalize_winner(x):
            if pd.isna(x):
                return ""
            s = str(x).strip().lower()
            # quelques alias possibles
            if s in ("white", "blanc"):
                return "white"
            if s in ("black", "noir"):
                return "black"
            if s in ("draw", "nulle", "null", "remis", "remise", "tie"):
                return "draw"
            return s
        df_gs[winner_col] = df_gs[winner_col].apply(normalize_winner)

    # --- fusion ---
    merge_cols = ["game_id"]
    for c in [moves_col, status_col, winner_col]:
        if c:
            merge_cols.append(c)

    games = (
        players
        .merge(df_gs[merge_cols], on="game_id", how="left")
        .merge(duration[["game_id", "duration"]], on="game_id", how="left")
    )

    # --- colonne vainqueur (couleur + nom) ---
    def pretty_winner(row):
        color_val = row.get(winner_col) if winner_col else ""
        color = "" if pd.isna(color_val) else str(color_val).strip().lower()
        if color == "white":
            return f"⚪ {row.get('white_player')}"
        if color == "black":
            return f"⚫ {row.get('black_player')}"
        if color == "draw":
            return "½–½"
        return "—"

    games["winner_pretty"] = games.apply(pretty_winner, axis=1)

    # --- colonnes à afficher ---
    display_cols = ["game_id", "white_player", "black_player"]
    if moves_col:  display_cols.append(moves_col)     # ex: "ptal_move"
    if status_col: display_cols.append(status_col)    # ex: "status"
    display_cols += ["winner_pretty", "duration"]

    games = games[display_cols].sort_values(by="game_id").reset_index(drop=True)
    return games

@app.route("/")
def index():
    games = load_games().to_dict(orient="records")
    return render_template("index.html", games=games)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
