# Trả lời 5 câu hỏi nộp bài (SUBMISSION.md)

## 1. Trade-offs kiến trúc: performance vs reliability vs maintainability

Kiến trúc hybrid tách phần rẻ/stateful (Kafka, Qdrant, Redis, Prometheus, Grafana,
Prefect orchestration) chạy local, và phần nặng GPU (vLLM inference) chạy trên
Kaggle free-tier GPU qua tunnel. Đổi lại performance tốt hơn với chi phí gần bằng
0, ta chấp nhận thêm độ trễ mạng và một điểm phụ thuộc bên ngoài (tunnel có thể
rớt, Kaggle session tự tắt sau vài giờ). Về reliability, thay vì thêm retry ở
từng script riêng lẻ, toàn bộ logic chịu lỗi được gom một chỗ trong API Gateway
(`search_context`/`call_llm` trong `api-gateway/main.py`) — Qdrant lỗi thì bỏ
qua context, vLLM lỗi thì trả lời "degraded" thay vì crash. Về maintainability,
Prefect flow dùng `.serve(cron=...)` (tự đăng ký + tự lên lịch trong một
process) thay vì mô hình work-pool/worker/deployment riêng biệt — ít khái niệm
phải đồng bộ hơn, đánh đổi lại là process đó là single point of failure cho
riêng job ingest này (chấp nhận được ở quy mô lab, sẽ cần work-pool thật nếu
scale lên nhiều flow).

## 2. Xử lý ngắt kết nối Local ↔ Kaggle, có fallback không?

Container API Gateway đọc `VLLM_URL`/`EMBED_NGROK_URL` qua `os.environ.get(...,
"")` thay vì `os.environ[...]` — trước đây thiếu biến này làm cả container crash
ngay lúc import, kéo theo cả health check và Prometheus scraping cũng chết theo.
Ở mức request, `call_llm()` bọc lời gọi vLLM trong try/except bắt
`httpx.HTTPError`/`TimeoutException`/`KeyError`, và trả về HTTP 200 với
`"degraded": true` cùng một câu trả lời báo lỗi rõ ràng, thay vì để lỗi 500 lan
ra ngoài. Chưa có cơ chế fallback sang một tunnel/GPU dự phòng thứ hai — đây là
giới hạn đã biết (một Kaggle notebook = một điểm lỗi duy nhất cho phần LLM); nếu
cần production-grade hơn, bước tiếp theo tự nhiên là giữ danh sách nhiều tunnel
URL và thử lần lượt.

## 3. Kafka giúp decouple các components như thế nào?

`scripts/01_ingest_to_kafka.py` (producer) không biết gì về Prefect hay ai đang
consume; `prefect/flows/kafka_to_delta.py` (consumer) không biết ai đã produce
hay có bao nhiêu producer. Nhờ vậy hai phía có thể down/redeploy độc lập —
message chỉ đơn giản nằm chờ trong topic `data.raw` tới lần chạy lịch (mỗi 5
phút, qua `.serve(cron=...)`) tiếp theo. So với gọi thẳng từ ingestion script
vào Prefect, cách này tránh việc ingestion phải chờ Prefect rảnh, và nếu Prefect
chậm/lỗi thì không chặn ngược lại ingestion. Kafka cũng cho khả năng replay tự
nhiên: nếu sửa bug trong `save_to_delta()`, có thể trỏ một consumer group mới
vào topic để xử lý lại toàn bộ message cũ còn nằm đó (nhờ
`auto_offset_reset="earliest"`).

## 4. Observability được implement như thế nào?

- **Logs:** `docker compose logs <service>` theo từng container — đủ dùng ở quy
  mô lab, chưa có log aggregation tập trung (ngoài phạm vi đợt sửa này).
- **Metrics:** `prometheus-fastapi-instrumentator` expose
  `http_requests_total{handler,method,status}` và
  `http_request_duration_seconds_bucket{handler,method,le}` tại
  `/metrics` trên API Gateway; Prometheus scrape đúng 15s/lần. File
  `monitoring/prometheus.yml` trước đây còn scrape cả `kafka:9092` và
  `prefect-orion:4200` — hai target này không hề expose endpoint dạng
  Prometheus nên luôn báo `up=0`; đã bỏ để điểm Observability phản ánh tín hiệu
  thật thay vì nhiễu đỏ vĩnh viễn.
- **Dashboards:** Grafana tự động provision datasource Prometheus và dashboard
  "Lab28 API Gateway" (request rate, P95 latency, error rate 5xx) ngay khi
  `docker compose up`, không cần dựng tay trước mỗi lần demo.
- **Traces:** LangSmith qua `scripts/09_verify_observability.py`, cần
  `LANGCHAIN_API_KEY` thật của học viên; script kiểm tra bằng
  `client.list_runs(project_name="lab28-platform", limit=1)`.

## 5. Nếu Qdrant hoặc Kafka crash, hệ thống xử lý ra sao? Có graceful degradation?

- **Qdrant crash:** `search_context()` bắt `httpx.HTTPError`/`TimeoutException`
  và trả về context rỗng; endpoint `/api/v1/chat` vẫn gọi LLM (không có context
  retrieval) và vẫn trả 200 — chất lượng câu trả lời giảm nhưng dịch vụ không
  down.
- **Kafka crash:** `KafkaConsumer(...)` trong task `consume_and_process` sẽ
  raise khi không connect được; task/flow run đó được Prefect đánh dấu
  "Failed" trong UI, nhưng vì chạy qua `.serve(cron=...)` nên process vẫn tiếp
  tục polling và tự thử lại ở lần chạy 5 phút kế tiếp — một lần Kafka chập chờn
  tự phục hồi mà không cần can thiệp, đánh đổi là mất đúng batch của lần chạy
  đó (không mất dữ liệu vì Kafka vẫn giữ message, chỉ là xử lý bị trễ; chưa có
  cơ chế dead-letter/backoff nào khác ngoài cơ chế retry mặc định của Prefect).
- `GET /health` không phụ thuộc Qdrant/Kafka/vLLM, nên health probe kiểu
  Docker/Kubernetes vẫn báo gateway "up" đúng ngay cả khi các dependency phía
  sau đang degraded.
