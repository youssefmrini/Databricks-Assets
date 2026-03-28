# Databricks notebook source
# MAGIC %md
# MAGIC # World Cup 2026 Prediction Pipeline v3
# MAGIC **Enhanced model with expanded training data, ensemble methods, and SHAP explanations**
# MAGIC
# MAGIC Improvements over v2:
# MAGIC - Expanded from 1,248 WC-only matches to ~20,000+ international matches (2000+)
# MAGIC - Ensemble of XGBoost + LightGBM + Logistic Regression
# MAGIC - Recency-weighted and tournament-weighted samples
# MAGIC - Bayesian-smoothed H2H features (fixes SHAP dominance issue)
# MAGIC - Optuna hyperparameter tuning (30 trials per model)
# MAGIC - Calibrated probabilities via isotonic regression
# MAGIC - Per-match SHAP explanations for 2026 predictions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Configuration & Setup

# COMMAND ----------

# MAGIC %pip install lightgbm xgboost optuna shap requests "numpy<2.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import requests
import json
import math
from datetime import datetime, timedelta
from collections import defaultdict
from io import StringIO

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, DoubleType, DateType, BooleanType
)

import xgboost as xgb
import lightgbm as lgb
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    accuracy_score, roc_auc_score, log_loss, brier_score_loss,
    classification_report, confusion_matrix
)
from sklearn.model_selection import StratifiedKFold
import shap

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
except ImportError:
    raise ImportError("optuna is required. Run: pip install optuna")

spark = SparkSession.builder.getOrCreate()

# Set catalog and schema
spark.sql("USE CATALOG main")
spark.sql("USE SCHEMA worldcup_v2")

# ============================================================
# TEAM NAME MAPPING (GitHub dataset -> Standard names used in app)
# ============================================================
TEAM_NAME_MAP = {
    "Korea Republic": "South Korea",
    "Côte d'Ivoire": "Ivory Coast",
    "Cote d'Ivoire": "Ivory Coast",
    "IR Iran": "Iran",
    "Korea DPR": "North Korea",
    "United States Virgin Islands": "US Virgin Islands",
    "Chinese Taipei": "Taiwan",
    "Türkiye": "Turkey",
    "Turkiye": "Turkey",
    "Cabo Verde": "Cape Verde",
    "Eswatini": "Eswatini",
    "Congo DR": "DR Congo",
    "Brunei Darussalam": "Brunei",
    "Kyrgyz Republic": "Kyrgyzstan",
    "St Kitts and Nevis": "Saint Kitts and Nevis",
    "St Lucia": "Saint Lucia",
    "St Vincent and the Grenadines": "Saint Vincent and the Grenadines",
    "Timor-Leste": "East Timor",
    "Curaçao": "Curacao",
    "Curacao": "Curacao",
}

def normalize_team_name(name):
    """Map dataset team names to standard app names."""
    if name in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[name]
    return name

# ============================================================
# WC 2026 GROUPS
# ============================================================
WC_2026_GROUPS = {
    "A": ["Mexico", "South Africa", "South Korea", "Denmark"],
    "B": ["Canada", "Italy", "Qatar", "Switzerland"],
    "C": ["Brazil", "Morocco", "Haiti", "Scotland"],
    "D": ["United States", "Paraguay", "Australia", "Turkey"],
    "E": ["Germany", "Curacao", "Ivory Coast", "Ecuador"],
    "F": ["Netherlands", "Japan", "Ukraine", "Tunisia"],
    "G": ["Belgium", "Egypt", "Iran", "New Zealand"],
    "H": ["Spain", "Cape Verde", "Saudi Arabia", "Uruguay"],
    "I": ["France", "Senegal", "Norway", "Bolivia"],
    "J": ["Argentina", "Algeria", "Austria", "Jordan"],
    "K": ["Portugal", "Colombia", "Uzbekistan", "Jamaica"],
    "L": ["England", "Croatia", "Ghana", "Panama"],
}

ALL_WC_2026_TEAMS = sorted(set(
    team for group in WC_2026_GROUPS.values() for team in group
))

# Confederation mapping
TEAM_CONFEDERATION = {
    # UEFA
    "Denmark": "UEFA", "Italy": "UEFA", "Switzerland": "UEFA", "Scotland": "UEFA",
    "Turkey": "UEFA", "Germany": "UEFA", "Netherlands": "UEFA",
    "Ukraine": "UEFA", "Belgium": "UEFA", "Spain": "UEFA",
    "France": "UEFA", "Norway": "UEFA", "Austria": "UEFA", "Portugal": "UEFA",
    "England": "UEFA", "Croatia": "UEFA",
    # CONMEBOL
    "Brazil": "CONMEBOL", "Paraguay": "CONMEBOL", "Colombia": "CONMEBOL",
    "Argentina": "CONMEBOL", "Bolivia": "CONMEBOL", "Uruguay": "CONMEBOL",
    "Ecuador": "CONMEBOL",
    # CONCACAF
    "Mexico": "CONCACAF", "Canada": "CONCACAF", "Haiti": "CONCACAF",
    "United States": "CONCACAF", "Curacao": "CONCACAF", "Jamaica": "CONCACAF",
    "Panama": "CONCACAF",
    # AFC
    "South Korea": "AFC", "Qatar": "AFC", "Japan": "AFC",
    "Australia": "AFC", "Iran": "AFC", "Saudi Arabia": "AFC",
    "Jordan": "AFC", "Uzbekistan": "AFC",
    # CAF
    "South Africa": "CAF", "Morocco": "CAF", "Ivory Coast": "CAF",
    "Tunisia": "CAF", "Egypt": "CAF", "Cape Verde": "CAF",
    "Senegal": "CAF", "Algeria": "CAF", "Ghana": "CAF",
    # OFC
    "New Zealand": "OFC",
}

CONF_TIER = {"UEFA": 1, "CONMEBOL": 2, "AFC": 3, "CAF": 4, "CONCACAF": 5, "OFC": 6}

# Tournament weight mapping
TOURNAMENT_WEIGHTS = {
    "FIFA World Cup": 1.0,
    "Confederations Cup": 0.85,
    "UEFA Euro": 0.85,
    "Copa América": 0.85,
    "African Cup of Nations": 0.85,
    "AFC Asian Cup": 0.85,
    "Gold Cup": 0.85,
    "UEFA Euro qualification": 0.6,
    "FIFA World Cup qualification": 0.7,
    "Copa América qualification": 0.6,
    "African Cup of Nations qualification": 0.6,
    "AFC Asian Cup qualification": 0.6,
    "CONCACAF Nations League": 0.6,
    "UEFA Nations League": 0.65,
    "Friendly": 0.4,
}

# K-factor for Elo by tournament type
ELO_K_FACTORS = {
    "FIFA World Cup": 60,
    "Confederations Cup": 50,
    "UEFA Euro": 50,
    "Copa América": 50,
    "African Cup of Nations": 50,
    "AFC Asian Cup": 50,
    "Gold Cup": 50,
    "UEFA Euro qualification": 40,
    "FIFA World Cup qualification": 40,
    "Copa América qualification": 40,
    "African Cup of Nations qualification": 40,
    "AFC Asian Cup qualification": 40,
    "CONCACAF Nations League": 40,
    "UEFA Nations League": 40,
    "Friendly": 20,
}

print("Pipeline v3 configuration loaded successfully.")
print(f"WC 2026 teams: {len(ALL_WC_2026_TEAMS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Scrape Expanded Training Data

# COMMAND ----------

# Download international football results dataset
url = "https://raw.githubusercontent.com/martj42/international_results/master/results.csv"
print(f"Downloading data from {url}...")

response = requests.get(url, timeout=60)
response.raise_for_status()
raw_df = pd.read_csv(StringIO(response.text))

print(f"Total matches downloaded: {len(raw_df)}")
print(f"Date range: {raw_df['date'].min()} to {raw_df['date'].max()}")
print(f"Columns: {list(raw_df.columns)}")

# Parse dates
raw_df["date"] = pd.to_datetime(raw_df["date"])

# Filter to matches from 2000 onwards (modern football era)
raw_df = raw_df[raw_df["date"] >= "2000-01-01"].copy()
print(f"Matches from 2000 onwards: {len(raw_df)}")

# Normalize team names
raw_df["home_team"] = raw_df["home_team"].apply(normalize_team_name)
raw_df["away_team"] = raw_df["away_team"].apply(normalize_team_name)

# Map tournament to weight
def get_tournament_weight(tournament):
    """Return weight for a tournament, defaulting to 0.4 for unknown."""
    if tournament in TOURNAMENT_WEIGHTS:
        return TOURNAMENT_WEIGHTS[tournament]
    t_lower = tournament.lower()
    if "world cup" in t_lower and "qualification" in t_lower:
        return 0.7
    elif "world cup" in t_lower:
        return 1.0
    elif "qualification" in t_lower:
        return 0.6
    elif any(comp in t_lower for comp in ["euro", "copa", "african cup", "asian cup", "gold cup"]):
        return 0.85
    elif "nations league" in t_lower:
        return 0.65
    elif "friendly" in t_lower:
        return 0.4
    return 0.5  # Unknown competitive tournament

raw_df["tournament_weight"] = raw_df["tournament"].apply(get_tournament_weight)

# Classify tournament type
def get_tournament_type(tournament):
    t_lower = tournament.lower()
    if "world cup" in t_lower and "qualification" not in t_lower:
        return "World Cup"
    elif "world cup" in t_lower and "qualification" in t_lower:
        return "WC Qualifier"
    elif any(x in t_lower for x in ["euro ", "copa am", "african cup", "asian cup", "gold cup"]):
        if "qualification" in t_lower:
            return "Continental Qualifier"
        return "Continental Final"
    elif "nations league" in t_lower:
        return "Nations League"
    elif "friendly" in t_lower:
        return "Friendly"
    elif "qualification" in t_lower:
        return "Other Qualifier"
    return "Other"

raw_df["tournament_type"] = raw_df["tournament"].apply(get_tournament_type)

# Create result column
def compute_result(row):
    if row["home_score"] > row["away_score"]:
        return 2  # Home win
    elif row["home_score"] == row["away_score"]:
        return 1  # Draw
    else:
        return 0  # Away win

raw_df["result"] = raw_df.apply(compute_result, axis=1)

# Add match_id
raw_df = raw_df.reset_index(drop=True)
raw_df["match_id"] = raw_df.index + 1

print(f"\nTournament type distribution:")
print(raw_df["tournament_type"].value_counts())
print(f"\nTotal matches to save: {len(raw_df)}")

# Save to Delta table
spark_df = spark.createDataFrame(raw_df)
spark_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("main.worldcup_v2.expanded_matches_raw")

print("\nSaved expanded_matches_raw table.")
display(spark.sql("SELECT tournament_type, COUNT(*) as cnt, ROUND(AVG(tournament_weight),2) as avg_weight FROM main.worldcup_v2.expanded_matches_raw GROUP BY tournament_type ORDER BY cnt DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Compute Elo Ratings (Full History)

# COMMAND ----------

# Load expanded matches
matches_df = spark.sql("""
    SELECT * FROM main.worldcup_v2.expanded_matches_raw ORDER BY date, match_id
""").toPandas()

print(f"Computing Elo ratings for {len(matches_df)} matches...")

# Elo rating system
class EloSystem:
    def __init__(self, default_rating=1500):
        self.ratings = defaultdict(lambda: default_rating)
        self.history = []
        self.default_rating = default_rating

    def expected_score(self, rating_a, rating_b):
        """Expected score for team A against team B."""
        return 1.0 / (1.0 + 10 ** ((rating_b - rating_a) / 400.0))

    def get_k_factor(self, tournament):
        """Get K-factor based on tournament type."""
        if tournament in ELO_K_FACTORS:
            return ELO_K_FACTORS[tournament]
        t_lower = tournament.lower()
        if "world cup" in t_lower and "qualification" not in t_lower:
            return 60
        elif "qualification" in t_lower:
            return 40
        elif any(x in t_lower for x in ["euro", "copa", "african", "asian", "gold cup"]):
            return 50
        elif "nations league" in t_lower:
            return 40
        elif "friendly" in t_lower:
            return 20
        return 30

    def goal_diff_multiplier(self, goal_diff):
        """Multiplier based on goal difference."""
        gd = abs(goal_diff)
        if gd <= 1:
            return 1.0
        elif gd == 2:
            return 1.5
        else:
            return (11.0 + gd) / 8.0

    def update(self, home_team, away_team, home_score, away_score, tournament, date, match_id, neutral=False):
        """Update Elo ratings after a match."""
        home_elo_before = self.ratings[home_team]
        away_elo_before = self.ratings[away_team]

        # Home advantage (reduced for neutral venues)
        home_advantage = 50 if not neutral else 0
        effective_home = home_elo_before + home_advantage

        # Expected scores
        exp_home = self.expected_score(effective_home, away_elo_before)
        exp_away = 1.0 - exp_home

        # Actual scores
        if home_score > away_score:
            actual_home, actual_away = 1.0, 0.0
        elif home_score == away_score:
            actual_home, actual_away = 0.5, 0.5
        else:
            actual_home, actual_away = 0.0, 1.0

        # K-factor and goal diff multiplier
        k = self.get_k_factor(tournament)
        gd_mult = self.goal_diff_multiplier(home_score - away_score)

        # Update ratings
        home_elo_after = home_elo_before + k * gd_mult * (actual_home - exp_home)
        away_elo_after = away_elo_before + k * gd_mult * (actual_away - exp_away)

        self.ratings[home_team] = home_elo_after
        self.ratings[away_team] = away_elo_after

        # Record history
        self.history.append({
            "match_id": match_id,
            "date": date,
            "home_team": home_team,
            "away_team": away_team,
            "tournament": tournament,
            "home_elo_before": round(home_elo_before, 2),
            "away_elo_before": round(away_elo_before, 2),
            "home_elo_after": round(home_elo_after, 2),
            "away_elo_after": round(away_elo_after, 2),
            "elo_diff": round(home_elo_before - away_elo_before, 2),
            "elo_win_prob_home": round(exp_home, 4),
            "k_factor": k,
        })

        return home_elo_before, away_elo_before, home_elo_after, away_elo_after

# Initialize Elo system and process all matches
elo = EloSystem(default_rating=1500)

for _, row in matches_df.iterrows():
    neutral = bool(row.get("neutral", False))
    elo.update(
        home_team=row["home_team"],
        away_team=row["away_team"],
        home_score=int(row["home_score"]),
        away_score=int(row["away_score"]),
        tournament=row["tournament"],
        date=row["date"],
        match_id=int(row["match_id"]),
        neutral=neutral,
    )

elo_history_df = pd.DataFrame(elo.history)
print(f"Elo history records: {len(elo_history_df)}")

# Current Elo ratings for WC 2026 teams
print("\nCurrent Elo ratings for WC 2026 teams:")
wc_elos = {team: round(elo.ratings[team], 1) for team in ALL_WC_2026_TEAMS}
wc_elos_sorted = sorted(wc_elos.items(), key=lambda x: x[1], reverse=True)
for i, (team, rating) in enumerate(wc_elos_sorted[:20], 1):
    print(f"  {i:2d}. {team:25s} {rating:.1f}")

# Save Elo history to Delta
spark_elo = spark.createDataFrame(elo_history_df)
spark_elo.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("main.worldcup_v2.elo_history_expanded")

print("\nSaved elo_history_expanded table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Compute Enhanced Features

# COMMAND ----------

# Load data
matches_df = spark.sql("""
    SELECT * FROM main.worldcup_v2.expanded_matches_raw ORDER BY date, match_id
""").toPandas()

elo_hist_df = spark.sql("""
    SELECT * FROM main.worldcup_v2.elo_history_expanded ORDER BY match_id
""").toPandas()

# Try to load supplementary tables (FIFA rankings, squad data)
try:
    fifa_rankings_df = spark.sql("""
        SELECT * FROM main.worldcup_v2.feat_current_fifa_rankings_2026
    """).toPandas()
    print(f"Loaded FIFA rankings: {len(fifa_rankings_df)} rows")
    has_fifa_rankings = True
except Exception as e:
    print(f"FIFA rankings table not available: {e}")
    fifa_rankings_df = pd.DataFrame()
    has_fifa_rankings = False

try:
    squad_value_df = spark.sql("""
        SELECT * FROM main.worldcup_v2.feat_squad_market_value_2026
    """).toPandas()
    print(f"Loaded squad market values: {len(squad_value_df)} rows")
    has_squad_value = True
except Exception as e:
    print(f"Squad market value table not available: {e}")
    squad_value_df = pd.DataFrame()
    has_squad_value = False

try:
    squad_stats_df = spark.sql("""
        SELECT * FROM main.worldcup_v2.feat_squad_stats
    """).toPandas()
    print(f"Loaded squad stats: {len(squad_stats_df)} rows")
    has_squad_stats = True
except Exception as e:
    print(f"Squad stats table not available: {e}")
    squad_stats_df = pd.DataFrame()
    has_squad_stats = False

print(f"\nTotal matches: {len(matches_df)}")

# ============================================================
# Build lookups
# ============================================================

# Merge Elo into matches
elo_merge = elo_hist_df[["match_id", "home_elo_before", "away_elo_before",
                          "elo_diff", "elo_win_prob_home"]].copy()
matches_df = matches_df.merge(elo_merge, on="match_id", how="left")

# FIFA ranking lookup (team -> rank)
fifa_rank_lookup = {}
if has_fifa_rankings:
    for _, row in fifa_rankings_df.iterrows():
        team_col = None
        for col in ["team", "team_name", "country"]:
            if col in fifa_rankings_df.columns:
                team_col = col
                break
        rank_col = None
        for col in ["rank", "fifa_rank", "ranking"]:
            if col in fifa_rankings_df.columns:
                rank_col = col
                break
        if team_col and rank_col:
            team_name = normalize_team_name(str(row[team_col]))
            fifa_rank_lookup[team_name] = int(row[rank_col])

# Squad value lookup (team -> total market value in millions)
squad_value_lookup = {}
if has_squad_value:
    for _, row in squad_value_df.iterrows():
        team_col = None
        for col in ["team", "team_name", "country"]:
            if col in squad_value_df.columns:
                team_col = col
                break
        val_col = None
        for col in ["total_market_value", "market_value", "squad_value"]:
            if col in squad_value_df.columns:
                val_col = col
                break
        if team_col and val_col:
            team_name = normalize_team_name(str(row[team_col]))
            try:
                squad_value_lookup[team_name] = float(row[val_col])
            except (ValueError, TypeError):
                pass

# ============================================================
# Feature computation
# ============================================================

# Sort matches by date
matches_df = matches_df.sort_values("date").reset_index(drop=True)
matches_df["date"] = pd.to_datetime(matches_df["date"])

# Pre-compute: team match history for form features
team_match_history = defaultdict(list)  # team -> list of (date, gf, ga, result, tournament, opponent)

# Pre-compute: H2H history
h2h_history = defaultdict(list)  # (team_a, team_b) sorted -> list of (date, a_goals, b_goals, a_team, b_team)

# Pre-compute: tournament experience
tournament_experience = defaultdict(lambda: defaultdict(int))  # team -> tournament_type -> count

# Pre-compute: cross-confederation performance
cross_conf_record = defaultdict(lambda: {"wins": 0, "draws": 0, "losses": 0, "matches": 0})

# Reference date for recency
REFERENCE_DATE = pd.Timestamp("2026-06-01")

# Feature computation for each match
features_list = []
total = len(matches_df)

for idx, row in matches_df.iterrows():
    if idx % 2000 == 0:
        print(f"Processing match {idx}/{total}...")

    match_id = row["match_id"]
    date = row["date"]
    home = row["home_team"]
    away = row["away_team"]
    home_score = int(row["home_score"])
    away_score = int(row["away_score"])
    tournament = row["tournament"]
    tournament_weight = row["tournament_weight"]
    result = int(row["result"])
    tournament_type = row["tournament_type"]

    # ---- Elo features ----
    home_elo = row.get("home_elo_before", 1500)
    away_elo = row.get("away_elo_before", 1500)
    elo_diff = row.get("elo_diff", 0)
    elo_win_prob = row.get("elo_win_prob_home", 0.5)
    if pd.isna(home_elo):
        home_elo = 1500
    if pd.isna(away_elo):
        away_elo = 1500
    if pd.isna(elo_diff):
        elo_diff = 0
    if pd.isna(elo_win_prob):
        elo_win_prob = 0.5

    # ---- Form features (last 10, recency-weighted) ----
    LAMBDA_FORM = 0.15

    def compute_form(team, current_date, n=10):
        history = team_match_history[team]
        recent = history[-n:] if len(history) >= n else history
        if not recent:
            return {
                "form_w_wins": 0, "form_w_draws": 0, "form_w_losses": 0,
                "form_w_gf": 0, "form_w_ga": 0, "form_w_pts": 0,
                "form_momentum": 0, "form_matches": 0,
            }
        w_wins, w_draws, w_losses, w_gf, w_ga, w_pts = 0, 0, 0, 0, 0, 0
        total_weight = 0
        for i, (mdate, gf, ga, res, tourn, opp) in enumerate(recent):
            days_ago = max((current_date - mdate).days, 1)
            weight = math.exp(-LAMBDA_FORM * (days_ago / 365.0))
            total_weight += weight
            w_gf += gf * weight
            w_ga += ga * weight
            if res == 2:
                w_wins += weight
                w_pts += 3 * weight
            elif res == 1:
                w_draws += weight
                w_pts += 1 * weight
            else:
                w_losses += weight

        if total_weight > 0:
            w_wins /= total_weight
            w_draws /= total_weight
            w_losses /= total_weight
            w_gf /= total_weight
            w_ga /= total_weight
            w_pts /= total_weight

        # Momentum: last 5 vs previous 5
        momentum = 0
        if len(recent) >= 6:
            last5 = recent[-5:]
            prev5 = recent[-10:-5] if len(recent) >= 10 else recent[:-5]
            l5_pts = sum(3 if r == 2 else (1 if r == 1 else 0) for _, _, _, r, _, _ in last5) / max(len(last5), 1)
            p5_pts = sum(3 if r == 2 else (1 if r == 1 else 0) for _, _, _, r, _, _ in prev5) / max(len(prev5), 1)
            momentum = l5_pts - p5_pts

        return {
            "form_w_wins": round(w_wins, 4),
            "form_w_draws": round(w_draws, 4),
            "form_w_losses": round(w_losses, 4),
            "form_w_gf": round(w_gf, 4),
            "form_w_ga": round(w_ga, 4),
            "form_w_pts": round(w_pts, 4),
            "form_momentum": round(momentum, 4),
            "form_matches": len(recent),
        }

    home_form = compute_form(home, date)
    away_form = compute_form(away, date)

    # ---- H2H features (Bayesian smoothed, last 10 meetings) ----
    def compute_h2h(team_a, team_b, perspective="home"):
        key = tuple(sorted([team_a, team_b]))
        meetings = h2h_history[key]
        recent_meetings = meetings[-10:] if len(meetings) >= 10 else meetings
        n_meetings = len(recent_meetings)
        if n_meetings == 0:
            return {
                "h2h_matches": 0,
                "h2h_smoothed_win_rate": 0.5,
                "h2h_avg_goal_diff": 0.0,
            }
        wins = 0
        total_gd = 0
        for mdate, a_goals, b_goals, a_team, b_team in recent_meetings:
            if perspective == "home":
                my_goals = a_goals if a_team == team_a else b_goals
                opp_goals = b_goals if a_team == team_a else a_goals
            else:
                my_goals = b_goals if a_team == team_a else a_goals
                opp_goals = a_goals if a_team == team_a else b_goals
            if my_goals > opp_goals:
                wins += 1
            total_gd += (my_goals - opp_goals)

        # Bayesian smoothed win rate: (wins + prior * pseudo_count) / (n + pseudo_count)
        pseudo_count = 2
        smoothed_rate = (wins + 0.5 * pseudo_count) / (n_meetings + pseudo_count)
        avg_gd = total_gd / n_meetings

        return {
            "h2h_matches": n_meetings,
            "h2h_smoothed_win_rate": round(smoothed_rate, 4),
            "h2h_avg_goal_diff": round(avg_gd, 4),
        }

    h2h_home = compute_h2h(home, away, perspective="home")

    # ---- Tournament experience ----
    home_tourn_exp = tournament_experience[home].get(tournament_type, 0)
    away_tourn_exp = tournament_experience[away].get(tournament_type, 0)

    # ---- Confederation tier ----
    home_conf = TEAM_CONFEDERATION.get(home, "Unknown")
    away_conf = TEAM_CONFEDERATION.get(away, "Unknown")
    home_conf_tier = CONF_TIER.get(home_conf, 5)
    away_conf_tier = CONF_TIER.get(away_conf, 5)
    same_confederation = 1 if home_conf == away_conf else 0

    # ---- Cross-confederation performance ----
    home_cross_conf = cross_conf_record[home]
    away_cross_conf = cross_conf_record[away]
    home_cross_win_rate = home_cross_conf["wins"] / max(home_cross_conf["matches"], 1)
    away_cross_win_rate = away_cross_conf["wins"] / max(away_cross_conf["matches"], 1)

    # ---- Recency weight ----
    years_before_ref = max((REFERENCE_DATE - date).days / 365.0, 0.01)
    recency_weight = math.exp(-0.1 * years_before_ref)

    # ---- FIFA rank features (static for 2026 predictions) ----
    home_fifa_rank = fifa_rank_lookup.get(home, 50)
    away_fifa_rank = fifa_rank_lookup.get(away, 50)
    fifa_rank_diff = away_fifa_rank - home_fifa_rank  # Positive = home ranked better

    # ---- Squad value features ----
    home_squad_value = squad_value_lookup.get(home, 0)
    away_squad_value = squad_value_lookup.get(away, 0)
    squad_value_diff = home_squad_value - away_squad_value

    # ---- Neutral venue ----
    is_neutral = 1 if row.get("neutral", False) else 0

    # ---- Build feature dict ----
    feat = {
        "match_id": match_id,
        "date": date,
        "home_team": home,
        "away_team": away,
        "home_score": home_score,
        "away_score": away_score,
        "tournament": tournament,
        "tournament_type": tournament_type,
        "tournament_weight": tournament_weight,
        "result": result,
        # Elo
        "home_elo": round(home_elo, 2),
        "away_elo": round(away_elo, 2),
        "elo_diff": round(elo_diff, 2),
        "elo_win_prob": round(elo_win_prob, 4),
        # Home form
        "h_form_w_wins": home_form["form_w_wins"],
        "h_form_w_draws": home_form["form_w_draws"],
        "h_form_w_losses": home_form["form_w_losses"],
        "h_form_w_gf": home_form["form_w_gf"],
        "h_form_w_ga": home_form["form_w_ga"],
        "h_form_w_pts": home_form["form_w_pts"],
        "h_form_momentum": home_form["form_momentum"],
        "h_form_matches": home_form["form_matches"],
        # Away form
        "a_form_w_wins": away_form["form_w_wins"],
        "a_form_w_draws": away_form["form_w_draws"],
        "a_form_w_losses": away_form["form_w_losses"],
        "a_form_w_gf": away_form["form_w_gf"],
        "a_form_w_ga": away_form["form_w_ga"],
        "a_form_w_pts": away_form["form_w_pts"],
        "a_form_momentum": away_form["form_momentum"],
        "a_form_matches": away_form["form_matches"],
        # Form diffs
        "form_pts_diff": round(home_form["form_w_pts"] - away_form["form_w_pts"], 4),
        "form_gd_diff": round(
            (home_form["form_w_gf"] - home_form["form_w_ga"]) -
            (away_form["form_w_gf"] - away_form["form_w_ga"]), 4
        ),
        "momentum_diff": round(home_form["form_momentum"] - away_form["form_momentum"], 4),
        # H2H (smoothed)
        "h2h_matches": h2h_home["h2h_matches"],
        "h2h_smoothed_win_rate": h2h_home["h2h_smoothed_win_rate"],
        "h2h_avg_goal_diff": h2h_home["h2h_avg_goal_diff"],
        # Tournament experience
        "home_tourn_exp": home_tourn_exp,
        "away_tourn_exp": away_tourn_exp,
        "tourn_exp_diff": home_tourn_exp - away_tourn_exp,
        # Confederation
        "home_conf_tier": home_conf_tier,
        "away_conf_tier": away_conf_tier,
        "conf_tier_diff": away_conf_tier - home_conf_tier,
        "same_confederation": same_confederation,
        # Cross-confederation
        "home_cross_conf_win_rate": round(home_cross_win_rate, 4),
        "away_cross_conf_win_rate": round(away_cross_win_rate, 4),
        "cross_conf_diff": round(home_cross_win_rate - away_cross_win_rate, 4),
        # Recency
        "recency_weight": round(recency_weight, 6),
        # FIFA rank
        "home_fifa_rank": home_fifa_rank,
        "away_fifa_rank": away_fifa_rank,
        "fifa_rank_diff": fifa_rank_diff,
        # Squad value
        "home_squad_value": home_squad_value,
        "away_squad_value": away_squad_value,
        "squad_value_diff": squad_value_diff,
        # Neutral
        "is_neutral": is_neutral,
        # Target
        "target": result,
    }
    features_list.append(feat)

    # ---- Update rolling histories AFTER feature extraction ----
    home_res = 2 if home_score > away_score else (1 if home_score == away_score else 0)
    away_res = 2 if away_score > home_score else (1 if home_score == away_score else 0)

    team_match_history[home].append((date, home_score, away_score, home_res, tournament, away))
    team_match_history[away].append((date, away_score, home_score, away_res, tournament, home))

    # H2H history
    key = tuple(sorted([home, away]))
    h2h_history[key].append((date, home_score, away_score, home, away))

    # Tournament experience
    tournament_experience[home][tournament_type] += 1
    tournament_experience[away][tournament_type] += 1

    # Cross-confederation record
    if home_conf != away_conf and home_conf != "Unknown" and away_conf != "Unknown":
        cross_conf_record[home]["matches"] += 1
        cross_conf_record[away]["matches"] += 1
        if home_score > away_score:
            cross_conf_record[home]["wins"] += 1
            cross_conf_record[away]["losses"] += 1
        elif home_score == away_score:
            cross_conf_record[home]["draws"] += 1
            cross_conf_record[away]["draws"] += 1
        else:
            cross_conf_record[away]["wins"] += 1
            cross_conf_record[home]["losses"] += 1

features_df = pd.DataFrame(features_list)
print(f"\nFeatures computed for {len(features_df)} matches.")
print(f"Feature columns: {len(features_df.columns)}")
print(f"\nTarget distribution:")
print(features_df["target"].value_counts().sort_index())

# Save to Delta table
spark_features = spark.createDataFrame(features_df)
spark_features.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("main.worldcup_v2.gold_match_features_v3")

print("\nSaved gold_match_features_v3 table.")
display(spark.sql("SELECT COUNT(*) as total_matches, MIN(date) as min_date, MAX(date) as max_date FROM main.worldcup_v2.gold_match_features_v3"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Train Enhanced Model (Ensemble with Optuna)

# COMMAND ----------

# Load feature table
features_df = spark.sql("SELECT * FROM main.worldcup_v2.gold_match_features_v3").toPandas()
features_df["date"] = pd.to_datetime(features_df["date"])

print(f"Total matches: {len(features_df)}")

# ============================================================
# Define feature columns (exclude IDs, names, dates, targets, meta)
# ============================================================
EXCLUDE_COLS = [
    "match_id", "date", "home_team", "away_team", "home_score", "away_score",
    "tournament", "tournament_type", "tournament_weight", "result", "target",
    "recency_weight",
]

FEATURE_COLS = [
    col for col in features_df.columns
    if col not in EXCLUDE_COLS
    and features_df[col].dtype in ["float64", "float32", "int64", "int32", "float", "int"]
]

print(f"Feature columns ({len(FEATURE_COLS)}):")
for c in FEATURE_COLS:
    print(f"  - {c}")

# ============================================================
# Temporal split: train on pre-2022, test on 2022 WC matches
# ============================================================
wc_2022_mask = (
    (features_df["tournament_type"] == "World Cup") &
    (features_df["date"] >= "2022-11-01") &
    (features_df["date"] <= "2022-12-31")
)

train_mask = features_df["date"] < "2022-11-01"

X_train = features_df.loc[train_mask, FEATURE_COLS].copy()
y_train = features_df.loc[train_mask, "target"].copy()
w_train = (
    features_df.loc[train_mask, "recency_weight"] *
    features_df.loc[train_mask, "tournament_weight"]
).copy()

X_test = features_df.loc[wc_2022_mask, FEATURE_COLS].copy()
y_test = features_df.loc[wc_2022_mask, "target"].copy()

print(f"\nTraining set: {len(X_train)} matches")
print(f"Test set (2022 WC): {len(X_test)} matches")
print(f"Train target distribution:\n{y_train.value_counts().sort_index()}")
print(f"Test target distribution:\n{y_test.value_counts().sort_index()}")

# Fill NaN with 0
X_train = X_train.fillna(0)
X_test = X_test.fillna(0)

# ============================================================
# Model 1: XGBoost with Optuna
# ============================================================
print("\n" + "=" * 60)
print("Training XGBoost with Optuna (30 trials)...")
print("=" * 60)

def xgb_objective(trial):
    params = {
        "objective": "multi:softprob",
        "num_class": 3,
        "eval_metric": "mlogloss",
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
        "gamma": trial.suggest_float("gamma", 0, 5),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        "random_state": 42,
        "verbosity": 0,
        "tree_method": "hist",
    }
    model = xgb.XGBClassifier(**params)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = []
    for train_idx, val_idx in cv.split(X_train, y_train):
        Xt, Xv = X_train.iloc[train_idx], X_train.iloc[val_idx]
        yt, yv = y_train.iloc[train_idx], y_train.iloc[val_idx]
        wt = w_train.iloc[train_idx]
        model.fit(Xt, yt, sample_weight=wt, verbose=False)
        pred_proba = model.predict_proba(Xv)
        scores.append(log_loss(yv, pred_proba))
    return np.mean(scores)

xgb_study = optuna.create_study(direction="minimize", study_name="xgb")
xgb_study.optimize(xgb_objective, n_trials=30, show_progress_bar=True)

print(f"Best XGBoost log_loss: {xgb_study.best_value:.4f}")
print(f"Best XGBoost params: {xgb_study.best_params}")

# Train final XGBoost
best_xgb_params = xgb_study.best_params.copy()
best_xgb_params.update({
    "objective": "multi:softprob",
    "num_class": 3,
    "eval_metric": "mlogloss",
    "random_state": 42,
    "verbosity": 0,
    "tree_method": "hist",
})
xgb_model = xgb.XGBClassifier(**best_xgb_params)
xgb_model.fit(X_train, y_train, sample_weight=w_train, verbose=False)

xgb_pred_proba = xgb_model.predict_proba(X_test)
xgb_pred = xgb_model.predict(X_test)
print(f"XGBoost Test Accuracy: {accuracy_score(y_test, xgb_pred):.4f}")
print(f"XGBoost Test Log Loss: {log_loss(y_test, xgb_pred_proba):.4f}")

# ============================================================
# Model 2: LightGBM with Optuna
# ============================================================
print("\n" + "=" * 60)
print("Training LightGBM with Optuna (30 trials)...")
print("=" * 60)

def lgb_objective(trial):
    params = {
        "objective": "multiclass",
        "num_class": 3,
        "metric": "multi_logloss",
        "max_depth": trial.suggest_int("max_depth", 3, 12),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "min_child_samples": trial.suggest_int("min_child_samples", 5, 50),
        "num_leaves": trial.suggest_int("num_leaves", 15, 127),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        "random_state": 42,
        "verbose": -1,
    }
    model = lgb.LGBMClassifier(**params)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = []
    for train_idx, val_idx in cv.split(X_train, y_train):
        Xt, Xv = X_train.iloc[train_idx], X_train.iloc[val_idx]
        yt, yv = y_train.iloc[train_idx], y_train.iloc[val_idx]
        wt = w_train.iloc[train_idx]
        model.fit(Xt, yt, sample_weight=wt)
        pred_proba = model.predict_proba(Xv)
        scores.append(log_loss(yv, pred_proba))
    return np.mean(scores)

lgb_study = optuna.create_study(direction="minimize", study_name="lgb")
lgb_study.optimize(lgb_objective, n_trials=30, show_progress_bar=True)

print(f"Best LightGBM log_loss: {lgb_study.best_value:.4f}")
print(f"Best LightGBM params: {lgb_study.best_params}")

# Train final LightGBM
best_lgb_params = lgb_study.best_params.copy()
best_lgb_params.update({
    "objective": "multiclass",
    "num_class": 3,
    "metric": "multi_logloss",
    "random_state": 42,
    "verbose": -1,
})
lgb_model = lgb.LGBMClassifier(**best_lgb_params)
lgb_model.fit(X_train, y_train, sample_weight=w_train)

lgb_pred_proba = lgb_model.predict_proba(X_test)
lgb_pred = lgb_model.predict(X_test)
print(f"LightGBM Test Accuracy: {accuracy_score(y_test, lgb_pred):.4f}")
print(f"LightGBM Test Log Loss: {log_loss(y_test, lgb_pred_proba):.4f}")

# ============================================================
# Model 3: Logistic Regression (baseline)
# ============================================================
print("\n" + "=" * 60)
print("Training Logistic Regression (baseline)...")
print("=" * 60)

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

lr_model = LogisticRegression(
    max_iter=2000, C=1.0, solver="lbfgs", multi_class="multinomial", random_state=42
)
lr_model.fit(X_train_scaled, y_train, sample_weight=w_train)

lr_pred_proba = lr_model.predict_proba(X_test_scaled)
lr_pred = lr_model.predict(X_test_scaled)
print(f"Logistic Regression Test Accuracy: {accuracy_score(y_test, lr_pred):.4f}")
print(f"Logistic Regression Test Log Loss: {log_loss(y_test, lr_pred_proba):.4f}")

# ============================================================
# Ensemble: Weighted average
# ============================================================
print("\n" + "=" * 60)
print("Building Ensemble (XGB=0.5, LGB=0.4, LR=0.1)...")
print("=" * 60)

ENSEMBLE_WEIGHTS = {"xgb": 0.5, "lgb": 0.4, "lr": 0.1}

ensemble_proba = (
    ENSEMBLE_WEIGHTS["xgb"] * xgb_pred_proba +
    ENSEMBLE_WEIGHTS["lgb"] * lgb_pred_proba +
    ENSEMBLE_WEIGHTS["lr"] * lr_pred_proba
)
ensemble_pred = np.argmax(ensemble_proba, axis=1)

ensemble_acc = accuracy_score(y_test, ensemble_pred)
ensemble_logloss = log_loss(y_test, ensemble_proba)

print(f"Ensemble Test Accuracy: {ensemble_acc:.4f}")
print(f"Ensemble Test Log Loss: {ensemble_logloss:.4f}")

# ROC AUC (one-vs-rest)
try:
    from sklearn.preprocessing import label_binarize
    y_test_bin = label_binarize(y_test, classes=[0, 1, 2])
    ensemble_roc_auc = roc_auc_score(y_test_bin, ensemble_proba, multi_class="ovr", average="weighted")
    print(f"Ensemble ROC AUC (weighted OVR): {ensemble_roc_auc:.4f}")
except Exception as e:
    ensemble_roc_auc = None
    print(f"ROC AUC computation failed: {e}")

# Brier score (per class, averaged)
brier_scores = []
for cls in range(3):
    y_cls = (y_test == cls).astype(int)
    brier_scores.append(brier_score_loss(y_cls, ensemble_proba[:, cls]))
avg_brier = np.mean(brier_scores)
print(f"Ensemble Avg Brier Score: {avg_brier:.4f}")

# ============================================================
# Calibration via Isotonic Regression
# ============================================================
print("\nCalibrating probabilities with isotonic regression...")

calibrated_xgb = CalibratedClassifierCV(xgb_model, method="isotonic", cv=5)
calibrated_xgb.fit(X_train, y_train, sample_weight=w_train)

calibrated_lgb = CalibratedClassifierCV(lgb_model, method="isotonic", cv=5)
calibrated_lgb.fit(X_train, y_train, sample_weight=w_train)

cal_xgb_proba = calibrated_xgb.predict_proba(X_test)
cal_lgb_proba = calibrated_lgb.predict_proba(X_test)

cal_ensemble_proba = (
    ENSEMBLE_WEIGHTS["xgb"] * cal_xgb_proba +
    ENSEMBLE_WEIGHTS["lgb"] * cal_lgb_proba +
    ENSEMBLE_WEIGHTS["lr"] * lr_pred_proba
)
cal_ensemble_pred = np.argmax(cal_ensemble_proba, axis=1)
cal_ensemble_acc = accuracy_score(y_test, cal_ensemble_pred)
cal_ensemble_logloss = log_loss(y_test, cal_ensemble_proba)

print(f"Calibrated Ensemble Accuracy: {cal_ensemble_acc:.4f}")
print(f"Calibrated Ensemble Log Loss: {cal_ensemble_logloss:.4f}")

# ============================================================
# SHAP values (on XGBoost, the dominant model)
# ============================================================
print("\nComputing SHAP values...")

explainer = shap.TreeExplainer(xgb_model)
shap_values = explainer.shap_values(X_test)

# For multiclass, shap_values is a list of arrays (one per class)
if isinstance(shap_values, list):
    mean_abs_shap = np.mean([np.abs(sv).mean(axis=0) for sv in shap_values], axis=0)
else:
    mean_abs_shap = np.abs(shap_values).mean(axis=0)

shap_importance_df = pd.DataFrame({
    "feature": FEATURE_COLS,
    "mean_abs_shap": mean_abs_shap,
}).sort_values("mean_abs_shap", ascending=False).reset_index(drop=True)

shap_importance_df["rank"] = range(1, len(shap_importance_df) + 1)
shap_importance_df["model_version"] = "v3_ensemble"

print("\nTop 15 SHAP features:")
print(shap_importance_df.head(15).to_string(index=False))

# Verify h2h is no longer dominating
top_shap = shap_importance_df.iloc[0]["mean_abs_shap"]
second_shap = shap_importance_df.iloc[1]["mean_abs_shap"]
print(f"\nTop/2nd SHAP ratio: {top_shap/second_shap:.2f}x (was 5.96x in v2)")

# ============================================================
# Save model metadata
# ============================================================
model_metadata = pd.DataFrame([{
    "model_version": "v3_ensemble",
    "created_at": datetime.now().isoformat(),
    "training_matches": int(len(X_train)),
    "test_matches": int(len(X_test)),
    "n_features": int(len(FEATURE_COLS)),
    "xgb_accuracy": float(accuracy_score(y_test, xgb_pred)),
    "lgb_accuracy": float(accuracy_score(y_test, lgb_pred)),
    "lr_accuracy": float(accuracy_score(y_test, lr_pred)),
    "ensemble_accuracy": float(ensemble_acc),
    "calibrated_accuracy": float(cal_ensemble_acc),
    "ensemble_log_loss": float(ensemble_logloss),
    "calibrated_log_loss": float(cal_ensemble_logloss),
    "roc_auc": float(ensemble_roc_auc) if ensemble_roc_auc else None,
    "avg_brier_score": float(avg_brier),
    "ensemble_weights": json.dumps(ENSEMBLE_WEIGHTS),
    "xgb_best_params": json.dumps(xgb_study.best_params),
    "lgb_best_params": json.dumps(lgb_study.best_params),
    "feature_columns": json.dumps(FEATURE_COLS),
    "top_shap_feature": str(shap_importance_df.iloc[0]["feature"]),
    "top_shap_ratio": float(top_shap / second_shap),
}])

spark.createDataFrame(model_metadata).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("main.worldcup_v2.model_metadata_v3")

# Save SHAP importance
spark.createDataFrame(shap_importance_df).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("main.worldcup_v2.shap_importance_v3")

print("\nSaved model_metadata_v3 and shap_importance_v3 tables.")

# ============================================================
# Store models in memory for subsequent cells
# ============================================================
# Variables that persist: calibrated_xgb, calibrated_lgb, lr_model, scaler,
# FEATURE_COLS, ENSEMBLE_WEIGHTS, xgb_model (for SHAP), explainer

print("\n" + "=" * 60)
print("MODEL TRAINING COMPLETE")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Backtest on 2022 World Cup

# COMMAND ----------

# Load 2022 WC matches from features table
features_df = spark.sql("SELECT * FROM main.worldcup_v2.gold_match_features_v3").toPandas()
features_df["date"] = pd.to_datetime(features_df["date"])

wc_2022 = features_df[
    (features_df["tournament_type"] == "World Cup") &
    (features_df["date"] >= "2022-11-01") &
    (features_df["date"] <= "2022-12-31")
].copy()

print(f"2022 World Cup matches for backtesting: {len(wc_2022)}")

if len(wc_2022) == 0:
    print("WARNING: No 2022 WC matches found. Checking available WC matches...")
    wc_matches = features_df[features_df["tournament_type"] == "World Cup"]
    print(f"Total WC matches: {len(wc_matches)}")
    if len(wc_matches) > 0:
        print(f"WC date range: {wc_matches['date'].min()} to {wc_matches['date'].max()}")
        latest_year = wc_matches["date"].dt.year.max()
        wc_2022 = wc_matches[wc_matches["date"].dt.year == latest_year].copy()
        print(f"Using {latest_year} WC matches: {len(wc_2022)}")

if len(wc_2022) > 0:
    X_bt = wc_2022[FEATURE_COLS].fillna(0)
    y_bt = wc_2022["target"]

    # Predict with ensemble
    cal_xgb_bt = calibrated_xgb.predict_proba(X_bt)
    cal_lgb_bt = calibrated_lgb.predict_proba(X_bt)

    X_bt_scaled = scaler.transform(X_bt)
    lr_bt = lr_model.predict_proba(X_bt_scaled)

    ensemble_bt = (
        ENSEMBLE_WEIGHTS["xgb"] * cal_xgb_bt +
        ENSEMBLE_WEIGHTS["lgb"] * cal_lgb_bt +
        ENSEMBLE_WEIGHTS["lr"] * lr_bt
    )
    pred_bt = np.argmax(ensemble_bt, axis=1)

    bt_accuracy = accuracy_score(y_bt, pred_bt)
    bt_logloss = log_loss(y_bt, ensemble_bt)

    print(f"\n2022 WC Backtest Results:")
    print(f"  Accuracy: {bt_accuracy:.4f}")
    print(f"  Log Loss: {bt_logloss:.4f}")
    print(f"\nClassification Report:")
    label_names = ["Away Win", "Draw", "Home Win"]
    print(classification_report(y_bt, pred_bt, target_names=label_names, zero_division=0))

    # Build backtest results table
    backtest_results = []
    for i, (_, row) in enumerate(wc_2022.iterrows()):
        actual = int(row["target"])
        predicted = int(pred_bt[i])
        correct = actual == predicted

        result_map = {0: "Away Win", 1: "Draw", 2: "Home Win"}
        backtest_results.append({
            "match_date": row["date"],
            "home_team": row["home_team"],
            "away_team": row["away_team"],
            "home_score": int(row["home_score"]),
            "away_score": int(row["away_score"]),
            "actual_result": result_map.get(actual, "Unknown"),
            "predicted_result": result_map.get(predicted, "Unknown"),
            "p_home_win": round(float(ensemble_bt[i, 2]), 4),
            "p_draw": round(float(ensemble_bt[i, 1]), 4),
            "p_away_win": round(float(ensemble_bt[i, 0]), 4),
            "correct": correct,
            "confidence": round(float(np.max(ensemble_bt[i])), 4),
            "model_version": "v3_ensemble",
        })

    backtest_df = pd.DataFrame(backtest_results)
    print(f"\nCorrect predictions: {backtest_df['correct'].sum()}/{len(backtest_df)} ({bt_accuracy:.1%})")

    display_df = backtest_df[["home_team", "away_team", "home_score", "away_score",
                               "actual_result", "predicted_result", "confidence", "correct"]].copy()
    print("\nDetailed Backtest Results:")
    print(display_df.to_string(index=False))

    # Save to Delta
    spark.createDataFrame(backtest_df).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable("main.worldcup_v2.backtest_2022_v3")
    print("\nSaved backtest_2022_v3 table.")
else:
    print("No WC matches available for backtesting. Skipping.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Generate 2026 Match Predictions with SHAP Explanations

# COMMAND ----------

# ============================================================
# Prepare current features for all 48 WC 2026 teams
# ============================================================

# Load latest features from the data
features_df = spark.sql("SELECT * FROM main.worldcup_v2.gold_match_features_v3").toPandas()
features_df["date"] = pd.to_datetime(features_df["date"])

# Get latest Elo ratings
elo_latest = spark.sql("SELECT * FROM main.worldcup_v2.elo_history_expanded").toPandas()

# Build team profiles from most recent data
def get_team_profile(team, df, elo_df):
    """Build a feature profile for a team from historical data."""
    home_matches = df[df["home_team"] == team].sort_values("date")
    away_matches = df[df["away_team"] == team].sort_values("date")

    # Latest Elo
    team_elo_home = elo_df[elo_df["home_team"] == team].sort_values("match_id")
    team_elo_away = elo_df[elo_df["away_team"] == team].sort_values("match_id")

    latest_elo = 1500
    latest_date = pd.Timestamp("2000-01-01")

    if len(team_elo_home) > 0:
        last_home = team_elo_home.iloc[-1]
        if pd.to_datetime(last_home["date"]) > latest_date:
            latest_elo = last_home["home_elo_after"]
            latest_date = pd.to_datetime(last_home["date"])
    if len(team_elo_away) > 0:
        last_away = team_elo_away.iloc[-1]
        if pd.to_datetime(last_away["date"]) > latest_date:
            latest_elo = last_away["away_elo_after"]
            latest_date = pd.to_datetime(last_away["date"])

    # Form (last 10 matches combined home+away)
    all_matches = []
    for _, row in home_matches.iterrows():
        res = 2 if row["home_score"] > row["away_score"] else (1 if row["home_score"] == row["away_score"] else 0)
        all_matches.append((row["date"], row["home_score"], row["away_score"], res))
    for _, row in away_matches.iterrows():
        res = 2 if row["away_score"] > row["home_score"] else (1 if row["home_score"] == row["away_score"] else 0)
        all_matches.append((row["date"], row["away_score"], row["home_score"], res))

    all_matches.sort(key=lambda x: x[0])
    recent = all_matches[-10:]

    LAMBDA = 0.15
    REFERENCE = pd.Timestamp("2026-06-01")

    w_wins, w_draws, w_losses, w_gf, w_ga, w_pts, total_w = 0, 0, 0, 0, 0, 0, 0
    for mdate, gf, ga, res in recent:
        days = max((REFERENCE - mdate).days, 1)
        w = math.exp(-LAMBDA * (days / 365.0))
        total_w += w
        w_gf += gf * w
        w_ga += ga * w
        if res == 2:
            w_wins += w
            w_pts += 3 * w
        elif res == 1:
            w_draws += w
            w_pts += w
        else:
            w_losses += w

    if total_w > 0:
        w_wins /= total_w
        w_draws /= total_w
        w_losses /= total_w
        w_gf /= total_w
        w_ga /= total_w
        w_pts /= total_w

    # Momentum
    momentum = 0
    if len(recent) >= 6:
        l5 = recent[-5:]
        p5 = recent[-10:-5] if len(recent) >= 10 else recent[:-5]
        l5_pts = sum(3 if r == 2 else (1 if r == 1 else 0) for _, _, _, r in l5) / max(len(l5), 1)
        p5_pts = sum(3 if r == 2 else (1 if r == 1 else 0) for _, _, _, r in p5) / max(len(p5), 1)
        momentum = l5_pts - p5_pts

    # Tournament experience (WC)
    wc_exp = len(home_matches[home_matches["tournament_type"] == "World Cup"]) + \
             len(away_matches[away_matches["tournament_type"] == "World Cup"])

    # Cross-confederation win rate
    conf = TEAM_CONFEDERATION.get(team, "Unknown")
    cross_wins, cross_total = 0, 0
    for _, row in home_matches.iterrows():
        opp_conf = TEAM_CONFEDERATION.get(row["away_team"], "Unknown")
        if opp_conf != conf and opp_conf != "Unknown":
            cross_total += 1
            if row["home_score"] > row["away_score"]:
                cross_wins += 1
    for _, row in away_matches.iterrows():
        opp_conf = TEAM_CONFEDERATION.get(row["home_team"], "Unknown")
        if opp_conf != conf and opp_conf != "Unknown":
            cross_total += 1
            if row["away_score"] > row["home_score"]:
                cross_wins += 1
    cross_rate = cross_wins / max(cross_total, 1)

    return {
        "elo": round(latest_elo, 2),
        "form_w_wins": round(w_wins, 4),
        "form_w_draws": round(w_draws, 4),
        "form_w_losses": round(w_losses, 4),
        "form_w_gf": round(w_gf, 4),
        "form_w_ga": round(w_ga, 4),
        "form_w_pts": round(w_pts, 4),
        "form_momentum": round(momentum, 4),
        "form_matches": len(recent),
        "tourn_exp": wc_exp,
        "conf_tier": CONF_TIER.get(conf, 5),
        "conf": conf,
        "cross_conf_win_rate": round(cross_rate, 4),
        "fifa_rank": fifa_rank_lookup.get(team, 50),
        "squad_value": squad_value_lookup.get(team, 0),
    }

# Build profiles for all 48 teams
print("Building team profiles for WC 2026 teams...")
team_profiles = {}
for team in ALL_WC_2026_TEAMS:
    team_profiles[team] = get_team_profile(team, features_df, elo_latest)
    print(f"  {team}: Elo={team_profiles[team]['elo']:.0f}, Form={team_profiles[team]['form_w_pts']:.2f}")

# ============================================================
# Generate predictions for all group stage matchups
# ============================================================

def get_h2h_for_pair(team_a, team_b, df):
    """Get H2H stats for team_a as home vs team_b."""
    h2h = df[
        ((df["home_team"] == team_a) & (df["away_team"] == team_b)) |
        ((df["home_team"] == team_b) & (df["away_team"] == team_a))
    ].sort_values("date").tail(10)

    n = len(h2h)
    if n == 0:
        return 0, 0.5, 0.0

    wins_a = 0
    gd_total = 0
    for _, row in h2h.iterrows():
        if row["home_team"] == team_a:
            if row["home_score"] > row["away_score"]:
                wins_a += 1
            gd_total += (row["home_score"] - row["away_score"])
        else:
            if row["away_score"] > row["home_score"]:
                wins_a += 1
            gd_total += (row["away_score"] - row["home_score"])

    pseudo = 2
    smoothed = (wins_a + 0.5 * pseudo) / (n + pseudo)
    avg_gd = gd_total / n

    return n, round(smoothed, 4), round(avg_gd, 4)

def build_match_features(home_team, away_team, team_profiles, df):
    """Build feature vector for a predicted match."""
    hp = team_profiles.get(home_team, {})
    ap = team_profiles.get(away_team, {})

    h2h_n, h2h_win, h2h_gd = get_h2h_for_pair(home_team, away_team, df)

    home_elo = hp.get("elo", 1500)
    away_elo = ap.get("elo", 1500)
    elo_diff = home_elo - away_elo

    # Elo win probability (neutral venue for WC)
    elo_win_prob = 1.0 / (1.0 + 10 ** ((-elo_diff) / 400.0))

    same_conf = 1 if hp.get("conf") == ap.get("conf") else 0

    feat = {
        "home_elo": home_elo,
        "away_elo": away_elo,
        "elo_diff": round(elo_diff, 2),
        "elo_win_prob": round(elo_win_prob, 4),
        "h_form_w_wins": hp.get("form_w_wins", 0),
        "h_form_w_draws": hp.get("form_w_draws", 0),
        "h_form_w_losses": hp.get("form_w_losses", 0),
        "h_form_w_gf": hp.get("form_w_gf", 0),
        "h_form_w_ga": hp.get("form_w_ga", 0),
        "h_form_w_pts": hp.get("form_w_pts", 0),
        "h_form_momentum": hp.get("form_momentum", 0),
        "h_form_matches": hp.get("form_matches", 0),
        "a_form_w_wins": ap.get("form_w_wins", 0),
        "a_form_w_draws": ap.get("form_w_draws", 0),
        "a_form_w_losses": ap.get("form_w_losses", 0),
        "a_form_w_gf": ap.get("form_w_gf", 0),
        "a_form_w_ga": ap.get("form_w_ga", 0),
        "a_form_w_pts": ap.get("form_w_pts", 0),
        "a_form_momentum": ap.get("form_momentum", 0),
        "a_form_matches": ap.get("form_matches", 0),
        "form_pts_diff": round(hp.get("form_w_pts", 0) - ap.get("form_w_pts", 0), 4),
        "form_gd_diff": round(
            (hp.get("form_w_gf", 0) - hp.get("form_w_ga", 0)) -
            (ap.get("form_w_gf", 0) - ap.get("form_w_ga", 0)), 4
        ),
        "momentum_diff": round(hp.get("form_momentum", 0) - ap.get("form_momentum", 0), 4),
        "h2h_matches": h2h_n,
        "h2h_smoothed_win_rate": h2h_win,
        "h2h_avg_goal_diff": h2h_gd,
        "home_tourn_exp": hp.get("tourn_exp", 0),
        "away_tourn_exp": ap.get("tourn_exp", 0),
        "tourn_exp_diff": hp.get("tourn_exp", 0) - ap.get("tourn_exp", 0),
        "home_conf_tier": hp.get("conf_tier", 5),
        "away_conf_tier": ap.get("conf_tier", 5),
        "conf_tier_diff": ap.get("conf_tier", 5) - hp.get("conf_tier", 5),
        "same_confederation": same_conf,
        "home_cross_conf_win_rate": hp.get("cross_conf_win_rate", 0),
        "away_cross_conf_win_rate": ap.get("cross_conf_win_rate", 0),
        "cross_conf_diff": round(
            hp.get("cross_conf_win_rate", 0) - ap.get("cross_conf_win_rate", 0), 4
        ),
        "home_fifa_rank": hp.get("fifa_rank", 50),
        "away_fifa_rank": ap.get("fifa_rank", 50),
        "fifa_rank_diff": ap.get("fifa_rank", 50) - hp.get("fifa_rank", 50),
        "home_squad_value": hp.get("squad_value", 0),
        "away_squad_value": ap.get("squad_value", 0),
        "squad_value_diff": hp.get("squad_value", 0) - ap.get("squad_value", 0),
        "is_neutral": 1,  # WC matches are neutral
    }
    return feat

# Generate all group stage matchups
print("\nGenerating 2026 WC group stage predictions...")

all_matchups = []
for group_name, teams in sorted(WC_2026_GROUPS.items()):
    for i in range(len(teams)):
        for j in range(i + 1, len(teams)):
            all_matchups.append({
                "group": group_name,
                "home_team": teams[i],
                "away_team": teams[j],
            })

print(f"Total group stage matchups: {len(all_matchups)}")

# Build feature matrix
match_features_list = []
for matchup in all_matchups:
    feat = build_match_features(
        matchup["home_team"], matchup["away_team"], team_profiles, features_df
    )
    match_features_list.append(feat)

pred_X = pd.DataFrame(match_features_list)

# Ensure columns match training features
for col in FEATURE_COLS:
    if col not in pred_X.columns:
        pred_X[col] = 0
pred_X = pred_X[FEATURE_COLS].fillna(0)

# Predict with calibrated ensemble
cal_xgb_pred = calibrated_xgb.predict_proba(pred_X)
cal_lgb_pred = calibrated_lgb.predict_proba(pred_X)
pred_X_scaled = scaler.transform(pred_X)
lr_pred_2026 = lr_model.predict_proba(pred_X_scaled)

ensemble_2026 = (
    ENSEMBLE_WEIGHTS["xgb"] * cal_xgb_pred +
    ENSEMBLE_WEIGHTS["lgb"] * cal_lgb_pred +
    ENSEMBLE_WEIGHTS["lr"] * lr_pred_2026
)

# ============================================================
# Compute per-match SHAP explanations
# ============================================================
print("Computing per-match SHAP explanations...")

shap_explainer = shap.TreeExplainer(xgb_model)
shap_vals_2026 = shap_explainer.shap_values(pred_X)

def get_top_shap(shap_values, feature_names, match_idx, top_n=5):
    """Get top N SHAP features for a specific match (home win class)."""
    if isinstance(shap_values, list):
        sv = shap_values[2][match_idx]  # Class 2 = home win
    else:
        sv = shap_values[match_idx]

    feat_shap = list(zip(feature_names, sv))
    feat_shap.sort(key=lambda x: abs(x[1]), reverse=True)

    top_features = []
    for fname, sval in feat_shap[:top_n]:
        direction = "+" if sval > 0 else "-" if sval < 0 else ""
        pct = abs(sval) / max(sum(abs(s) for _, s in feat_shap), 1e-8) * 100
        top_features.append(f"{fname}: {direction}{pct:.1f}%")

    return json.dumps(top_features)

# ============================================================
# Build prediction table with xG estimates
# ============================================================

def proba_to_xg(p_home, p_draw, p_away, total_goals_avg=2.5):
    """Convert probabilities to expected goals using Poisson approximation."""
    home_strength = p_home + 0.5 * p_draw
    away_strength = p_away + 0.5 * p_draw
    total = home_strength + away_strength
    xg_home = total_goals_avg * home_strength / total
    xg_away = total_goals_avg * away_strength / total
    return round(xg_home, 2), round(xg_away, 2)

predictions_2026 = []
for i, matchup in enumerate(all_matchups):
    p_away = float(ensemble_2026[i, 0])
    p_draw = float(ensemble_2026[i, 1])
    p_home = float(ensemble_2026[i, 2])
    confidence = float(max(p_home, p_draw, p_away))

    xg_home, xg_away = proba_to_xg(p_home, p_draw, p_away)

    shap_top5 = get_top_shap(shap_vals_2026, FEATURE_COLS, i, top_n=5)

    predictions_2026.append({
        "group": matchup["group"],
        "home_team": matchup["home_team"],
        "away_team": matchup["away_team"],
        "p_home_win": round(p_home, 4),
        "p_draw": round(p_draw, 4),
        "p_away_win": round(p_away, 4),
        "xg_home": xg_home,
        "xg_away": xg_away,
        "elo_home": team_profiles[matchup["home_team"]]["elo"],
        "elo_away": team_profiles[matchup["away_team"]]["elo"],
        "confidence": round(confidence, 4),
        "features_used": int(len(FEATURE_COLS)),
        "shap_top5": shap_top5,
        "model_version": "v3_ensemble",
    })

pred_2026_df = pd.DataFrame(predictions_2026)

# Display sample predictions
print("\nSample Group Stage Predictions:")
for group in ["A", "D", "H", "J"]:
    group_preds = pred_2026_df[pred_2026_df["group"] == group]
    print(f"\n  Group {group}:")
    for _, row in group_preds.iterrows():
        outcome = "HOME" if row["p_home_win"] > max(row["p_draw"], row["p_away_win"]) \
            else ("DRAW" if row["p_draw"] > row["p_away_win"] else "AWAY")
        print(f"    {row['home_team']:20s} vs {row['away_team']:20s}  "
              f"H:{row['p_home_win']:.1%} D:{row['p_draw']:.1%} A:{row['p_away_win']:.1%}  "
              f"xG: {row['xg_home']:.1f}-{row['xg_away']:.1f}  [{outcome}]")

# Save to Delta
spark.createDataFrame(pred_2026_df).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("main.worldcup_v2.match_probabilities_2026_v3")

print(f"\nSaved match_probabilities_2026_v3 ({len(pred_2026_df)} matchups).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Generate Team Power Rankings

# COMMAND ----------

# ============================================================
# Compute composite power index for all 48 WC 2026 teams
# ============================================================

def normalize_to_100(values, higher_is_better=True):
    """Normalize a dict of {team: value} to 0-100 scale."""
    vals = list(values.values())
    if not vals:
        return {k: 50 for k in values}
    min_v, max_v = min(vals), max(vals)
    if max_v == min_v:
        return {k: 50 for k in values}
    normalized = {}
    for k, v in values.items():
        score = (v - min_v) / (max_v - min_v) * 100
        if not higher_is_better:
            score = 100 - score
        normalized[k] = round(score, 2)
    return normalized

# Gather raw values
elo_scores = {team: team_profiles[team]["elo"] for team in ALL_WC_2026_TEAMS}
form_scores = {team: team_profiles[team]["form_w_pts"] for team in ALL_WC_2026_TEAMS}
tourn_exp_scores = {team: team_profiles[team]["tourn_exp"] for team in ALL_WC_2026_TEAMS}
squad_val_scores = {team: team_profiles[team]["squad_value"] for team in ALL_WC_2026_TEAMS}
fifa_rank_scores = {team: team_profiles[team]["fifa_rank"] for team in ALL_WC_2026_TEAMS}

# Normalize
elo_norm = normalize_to_100(elo_scores, higher_is_better=True)
form_norm = normalize_to_100(form_scores, higher_is_better=True)
tourn_norm = normalize_to_100(tourn_exp_scores, higher_is_better=True)
squad_norm = normalize_to_100(squad_val_scores, higher_is_better=True)
rank_norm = normalize_to_100(fifa_rank_scores, higher_is_better=False)

# Composite power index: Elo(40%) + Form(25%) + Experience(15%) + Squad(10%) + FIFA(10%)
power_rankings = []
for team in ALL_WC_2026_TEAMS:
    power_index = (
        0.40 * elo_norm[team] +
        0.25 * form_norm[team] +
        0.15 * tourn_norm[team] +
        0.10 * squad_norm[team] +
        0.10 * rank_norm[team]
    )

    team_group = "?"
    for g, teams in WC_2026_GROUPS.items():
        if team in teams:
            team_group = g
            break

    power_rankings.append({
        "team": team,
        "group": team_group,
        "power_index": round(power_index, 2),
        "elo_rating": round(elo_scores[team], 1),
        "elo_score": round(elo_norm[team], 2),
        "form_score": round(form_norm[team], 2),
        "tourn_exp_score": round(tourn_norm[team], 2),
        "squad_quality_score": round(squad_norm[team], 2),
        "fifa_rank_score": round(rank_norm[team], 2),
        "fifa_rank": int(fifa_rank_scores[team]),
        "confederation": TEAM_CONFEDERATION.get(team, "Unknown"),
        "model_version": "v3_ensemble",
    })

power_df = pd.DataFrame(power_rankings).sort_values("power_index", ascending=False).reset_index(drop=True)
power_df["rank"] = range(1, len(power_df) + 1)

print("WC 2026 Power Rankings:")
print("=" * 90)
for _, row in power_df.iterrows():
    print(f"  {row['rank']:3d}. {row['team']:25s} (Group {row['group']})  "
          f"Power: {row['power_index']:5.1f}  Elo: {row['elo_rating']:6.0f}  "
          f"FIFA: {row['fifa_rank']:3d}  Conf: {row['confederation']}")

# ============================================================
# Identify Dark Horses
# ============================================================
elo_ranked = power_df.sort_values("elo_rating", ascending=False).reset_index(drop=True)
elo_ranked["elo_rank"] = range(1, len(elo_ranked) + 1)

form_ranked = power_df.sort_values("form_score", ascending=False).reset_index(drop=True)
form_ranked["form_rank"] = range(1, len(form_ranked) + 1)

elo_rank_map = dict(zip(elo_ranked["team"], elo_ranked["elo_rank"]))
form_rank_map = dict(zip(form_ranked["team"], form_ranked["form_rank"]))

dark_horses = []
for team in ALL_WC_2026_TEAMS:
    er = elo_rank_map[team]
    fr = form_rank_map[team]
    if er >= 20 and fr <= 15:
        dark_horses.append({
            "team": team,
            "elo_rank": er,
            "form_rank": fr,
            "elo_rating": elo_scores[team],
            "form_score": form_norm[team],
        })

# Add dark_horse flag
power_df["is_dark_horse"] = power_df["team"].isin([dh["team"] for dh in dark_horses])

print(f"\nDark Horse Teams (Elo rank >= 20, Form rank <= 15):")
if dark_horses:
    for dh in sorted(dark_horses, key=lambda x: x["form_rank"]):
        print(f"  {dh['team']:25s}  Elo rank: {dh['elo_rank']:2d}  Form rank: {dh['form_rank']:2d}")
else:
    print("  No teams meet the strict dark horse criteria.")
    # Relaxed criteria
    relaxed = []
    for team in ALL_WC_2026_TEAMS:
        er = elo_rank_map[team]
        fr = form_rank_map[team]
        if er >= 15 and fr <= 20 and er - fr >= 5:
            relaxed.append({"team": team, "elo_rank": er, "form_rank": fr})
    if relaxed:
        print("  Relaxed criteria (Elo >= 15, Form <= 20, gap >= 5):")
        for r in sorted(relaxed, key=lambda x: x["form_rank"]):
            print(f"    {r['team']:25s}  Elo rank: {r['elo_rank']:2d}  Form rank: {r['form_rank']:2d}")

# Save to Delta
spark.createDataFrame(power_df).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("main.worldcup_v2.team_power_rankings_2026")

print(f"\nSaved team_power_rankings_2026 ({len(power_df)} teams).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Summary Output

# COMMAND ----------

# ============================================================
# Pipeline v3 Summary
# ============================================================

print("=" * 80)
print("WORLD CUP 2026 PREDICTION PIPELINE v3 - SUMMARY")
print("=" * 80)

print("\n--- DATA IMPROVEMENTS ---")
print(f"  Training data: ~{len(X_train):,} matches (vs 1,248 in v2)")
print(f"  Data range: 2000-2025 (vs 1930-2022 WC-only in v2)")
print(f"  Includes: WC, Continental, Qualifiers, Friendlies (weighted)")
print(f"  Tournament-weighted + recency-weighted samples")

print("\n--- MODEL IMPROVEMENTS ---")
print(f"  Old model: XGBoost only, 75% accuracy, 0.93 ROC AUC")
print(f"  New model: Ensemble (XGB {ENSEMBLE_WEIGHTS['xgb']:.0%} + "
      f"LGB {ENSEMBLE_WEIGHTS['lgb']:.0%} + LR {ENSEMBLE_WEIGHTS['lr']:.0%})")
print(f"  Ensemble accuracy: {ensemble_acc:.1%}")
print(f"  Calibrated accuracy: {cal_ensemble_acc:.1%}")
print(f"  Ensemble log loss: {ensemble_logloss:.4f}")
if ensemble_roc_auc:
    print(f"  ROC AUC: {ensemble_roc_auc:.4f}")
print(f"  Avg Brier score: {avg_brier:.4f}")
print(f"  Optuna tuning: 30 trials per model")
print(f"  Probability calibration: isotonic regression")

print("\n--- FEATURE IMPROVEMENTS ---")
print(f"  Total features: {len(FEATURE_COLS)}")
print(f"  H2H: Bayesian-smoothed (prior=0.5, pseudo_count=2), capped at 10 meetings")
print(f"  Form: Recency-weighted (lambda=0.15), includes momentum")
print(f"  Top SHAP feature: {shap_importance_df.iloc[0]['feature']} "
      f"({shap_importance_df.iloc[0]['mean_abs_shap']:.4f})")
print(f"  Top/2nd ratio: {top_shap/second_shap:.2f}x (was 5.96x in v2)")

if len(wc_2022) > 0:
    print(f"\n--- 2022 WC BACKTEST ---")
    print(f"  Matches tested: {len(wc_2022)}")
    print(f"  Accuracy: {bt_accuracy:.1%}")
    print(f"  Log loss: {bt_logloss:.4f}")

print(f"\n--- 2026 PREDICTIONS ---")
print(f"  Group stage matchups predicted: {len(pred_2026_df)}")
print(f"  Teams ranked: {len(power_df)}")

# Top 5 favorites
print(f"\n  Top 5 Favorites:")
for _, row in power_df.head(5).iterrows():
    print(f"    {row['rank']}. {row['team']} (Power: {row['power_index']:.1f}, Elo: {row['elo_rating']:.0f})")

# Dark horses
print(f"\n  Dark Horses:")
dh_teams = power_df[power_df["is_dark_horse"]]
if len(dh_teams) > 0:
    for _, row in dh_teams.iterrows():
        print(f"    - {row['team']} (Power rank: {row['rank']}, Group {row['group']})")
else:
    print(f"    No strict dark horses identified. See relaxed criteria in Cell 8.")

print(f"\n--- TABLES CREATED/UPDATED ---")
tables = [
    "main.worldcup_v2.expanded_matches_raw",
    "main.worldcup_v2.elo_history_expanded",
    "main.worldcup_v2.gold_match_features_v3",
    "main.worldcup_v2.model_metadata_v3",
    "main.worldcup_v2.shap_importance_v3",
    "main.worldcup_v2.backtest_2022_v3",
    "main.worldcup_v2.match_probabilities_2026_v3",
    "main.worldcup_v2.team_power_rankings_2026",
]
for t in tables:
    print(f"  - {t}")

print("\n" + "=" * 80)
print("Pipeline v3 complete. Upload to Databricks workspace e2-demo-field-eng to run.")
print("=" * 80)
