docker run --rm \
  -e LAKE_NAME="Lake Chad" \
  -e BBOX="[13.1, 12.5, 15.1, 14.5]" \
  -e START_DATE="2025-01-01" \
  -e END_DATE="2025-07-01" \
  lake-worker