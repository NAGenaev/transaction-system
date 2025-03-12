#!/bin/bash

# Параметры для транзакции
URL="http://localhost:8000/transactions"
AMOUNT="0.01"
SENDER="123e4567-e89b-12d3-a456-426614174001"
RECIPIENT="123e4567-e89b-12d3-a456-426614174000"

# Повторить транзакцию 10000 раз
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