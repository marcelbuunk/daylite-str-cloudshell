FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:slim
WORKDIR /app
COPY backfill_last3h.sh /app/backfill_last3h.sh
RUN chmod +x /app/backfill_last3h.sh
CMD ["/app/backfill_last3h.sh"]
