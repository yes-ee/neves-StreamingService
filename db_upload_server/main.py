import time
import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from ddtrace import patch

import config
import global_state
import metrics
from workers import PrimaryWorker, RetryScheduler, RetryWorker
from scanner import main_scanner_loop

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', force=True)

# 스캔 -> 메인 스레드
# 큐 구조 설명:
# 1번 큐
# WORK_QUEUE: 1차 처리 작업 큐
# 2번 큐
# RETRY_QUEUE: 재시도 작업 큐
# 3번 큐
# SCHEDULER_QUEUE: 실패 작업 대기 큐
# FIFO 보장, 1분마다 스케줄러가 확인하여 재시도 시간이 된 작업을 꺼내 3번 큐로 이동

def scanner_loop():
    """기존의 while True 스캐너 루프를 백그라운드 스레드로 분리"""
    logging.info("메인 스캐너 루프를 시작합니다.")
    while not global_state.SHUTDOWN_EVENT.is_set():
        main_scanner_loop()
        logging.info(
            f"다음 스캔까지 {config.SCAN_INTERVAL_SECONDS}초 대기... "
            f"(1차 큐: {global_state.WORK_QUEUE.qsize()}, "
            f"스케줄 큐: {global_state.SCHEDULER_QUEUE.qsize()}, "
            f"재시도 큐: {global_state.RETRY_QUEUE.qsize()})"
        )
        # sleep 대신 SHUTDOWN_EVENT 대기를 사용하여 즉시 종료 가능하게 함
        global_state.SHUTDOWN_EVENT.wait(config.SCAN_INTERVAL_SECONDS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("애플리케이션 시작...")
    logging.info(f"기본 녹화 폴더: {config.BASE_DIR}")
    logging.info(f"1차 처리 워커 수: {config.NUM_WORKERS}")
    logging.info(f"재시도 워커 수: {config.NUM_RETRY_WORKER}")
    logging.info(f"S3 버킷 이름: {config.S3_BUCKET_NAME}")
    logging.info(f"상태 서버 URL: {config.STATUS_SERVER_URL}")
    logging.info(f"스캔 간격(초): {config.SCAN_INTERVAL_SECONDS}")
    logging.info(f"최대 재시도 횟수: {config.MAX_RETRIES}")
    logging.info(f"재시도 지연 시간(분): {config.RETRY_DELAY_MINUTES}")
    logging.info(f"스케줄러 간격(초): {config.SCHEDULER_INTERVAL_SECONDS}")

    metrics.start_metrics_server()

    # 워커 스레드를 데몬으로 실행하여 메인 스레드 종료 시 함께 종료되도록 설정
    logging.info(f"{config.NUM_WORKERS}개의 1차 처리 워커를 시작합니다.")
    for _ in range(config.NUM_WORKERS):
        w = PrimaryWorker()
        w.daemon = True
        w.start()

    logging.info("재시도 스케줄러 스레드를 시작합니다.")
    scheduler = RetryScheduler()
    scheduler.daemon = True
    scheduler.start()

    logging.info(f"{config.NUM_RETRY_WORKER}개의 재시도 워커를 시작합니다.")
    for _ in range(config.NUM_RETRY_WORKER):
        rw = RetryWorker()
        rw.daemon = True
        rw.start()

    # 백그라운드 스캐너 스레드 시작
    scanner_thread = threading.Thread(target=scanner_loop, daemon=True)
    scanner_thread.start()

    yield  # FastAPI 애플리케이션 실행 구간

    logging.info("애플리케이션 종료 중... 백그라운드 스레드 종료 신호 전달.")
    global_state.SHUTDOWN_EVENT.set()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "queues": {
            "primary": global_state.WORK_QUEUE.qsize(),
            "scheduler": global_state.SCHEDULER_QUEUE.qsize(),
            "retry": global_state.RETRY_QUEUE.qsize()
        }
    }

if __name__ == "__main__":
    import uvicorn
    # 직접 실행 시 포트 8080에서 서버를 띄웁니다.
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=False)
