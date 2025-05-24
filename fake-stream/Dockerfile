FROM python:3.10

# Đặt thư mục làm việc mặc định
WORKDIR /opt/fake-stream

# Sao chép mã nguồn nếu có
COPY . /opt/fake-stream

# Cài đặt thư viện cần thiết
RUN pip install --no-cache-dir \
    pandas \
    requests \
    httpx \
    pyyaml \
    kafka-python \
    tqdm \
    numpy \
    pyarrow \
    fastparquet

# Giữ container chạy
CMD ["sleep", "infinity"]
