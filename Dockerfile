FROM rust:1.71-slim
WORKDIR /app
COPY . .
RUN cargo install --path .
CMD ["api-exec-database"]