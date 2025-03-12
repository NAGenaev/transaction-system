#!/bin/bash

# Параметры для транзакции
URL="http://localhost:8000/transactions"
AMOUNT="10.00"
SENDER="123e4567-e89b-12d3-a456-426614174003"
RECIPIENT="123e4567-e89b-12d3-a456-426614174002"

# Повторить транзакцию 1000 раз
for i in {1..1000}
do
  echo "Отправка транзакции №$i"
  curl -X 'POST' \
    "$URL" \
    -H 'Content-Type: application/json' \
    -d "{
      \"amount\": \"$AMOUNT\",
      \"sender\": \"$SENDER\",
      \"recipient\": \"$RECIPIENT\"
    }"
  echo -e "\n---\n"
done