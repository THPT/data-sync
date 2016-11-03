#!/bin/sh
echo "DUMP DATABASE"
echo "---"
echo ""
./data-sync export config/tables/product.json
./data-sync export config/tables/selling_order.json
./data-sync export config/tables/selling_store.json
./data-sync export config/tables/user.json
./data-sync export config/tables/video.json

echo "IMPORT DATABASE"
echo "---"
echo ""
./data-sync import config/tables/product.json product.tsv
./data-sync import config/tables/selling_order.json selling_order.tsv
./data-sync import config/tables/selling_store.json selling_store.tsv
./data-sync import config/tables/user.json user.tsv
./data-sync import config/tables/video.json video.tsv

