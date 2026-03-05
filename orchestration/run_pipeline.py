import subprocess
import sys

def run_step(step_name, command):
    print(f"\n🚀 Ejecutando: {step_name}")

    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        print(f"Error en {step_name}")
        sys.exit(1)

    print(f"{step_name} completado")


def main():
    run_step(
        "RAW → BRONZE",
        "python ingestion/batch/ingest_raw_to_bronze.py"
    )

    run_step(
        "BRONZE → SILVER",
        "python ingestion/batch/bronze_to_silver.py"
    )

    run_step(
        "SILVER → GOLD",
        "python ingestion/batch/silver_to_gold.py"
    )

    print("\n🎉 Pipeline completado exitosamente")


if __name__ == "__main__":
    main()

