import subprocess
import sys
import logging
from datetime import datetime


# configurar logging
log_file = f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def run_step(step_name, command):

    print(f"\n🚀 Ejecutando: {step_name}")
    logging.info(f"Starting step: {step_name}")

    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        logging.error(f"Error in step: {step_name}")
        print(f"❌ Error en {step_name}")
        sys.exit(1)

    logging.info(f"Completed step: {step_name}")
    print(f"✅ {step_name} completado")


def main():

    logging.info("Pipeline started")

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

    logging.info("Pipeline completed successfully")
    print("\n🎉 Pipeline completado exitosamente")


if __name__ == "__main__":
    main()