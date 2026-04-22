from pipeline.orchestrator import run_pipeline


if __name__ == "__main__":
    stats = run_pipeline()
    print("Pipeline run successful")
    print(stats)
