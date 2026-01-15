from pathlib import Path

# Project base directory (energy-data-pipeline/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data base directory
DATA_BASE_PATH = PROJECT_ROOT / "data"

# Medallion layers
BRONZE_PATH = DATA_BASE_PATH / "bronze"
SILVER_PATH = DATA_BASE_PATH / "silver"
GOLD_PATH = DATA_BASE_PATH / "gold"

# Ensure directories exist
BRONZE_PATH.mkdir(parents=True, exist_ok=True)
SILVER_PATH.mkdir(parents=True, exist_ok=True)
GOLD_PATH.mkdir(parents=True, exist_ok=True)

# Build filesystem path for a bronze Delta table
def bronze_table_path(table_name: str) -> str:
    
    return str(BRONZE_PATH / table_name)

# Builderfilesystem path for a silver Delta table
def silver_table_path(table_name: str) -> str:
    
    return str(SILVER_PATH / table_name)

# Build filesystem path for a gold Delta table
def gold_table_path(table_name: str) -> str:

    return str(GOLD_PATH / table_name)
