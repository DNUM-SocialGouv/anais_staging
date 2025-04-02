from pipeline.duckdb_pipeline import DuckDBPipeline

# Exécution du script avec la classe
if __name__ == "__main__":
    loader = DuckDBPipeline()
    loader.run()
    loader.list_tables()
    loader.close()
