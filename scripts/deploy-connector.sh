#!/bin/sh
echo '⏳ Attente du démarrage complet de Kafka Connect...'

until curl -s -f http://kafka-connect:8083/ > /dev/null; do
  echo '   Kafka Connect pas encore prêt...'
  sleep 3
done

echo '✅ API Kafka Connect disponible'

echo '📦 Vérification du plugin S3...'
until curl -s http://kafka-connect:8083/connector-plugins | grep -q 'S3SinkConnector'; do
  echo '   Plugin S3 pas encore chargé...'
  sleep 3
done

echo '✅ Plugin S3 trouvé'

echo '📝 Déploiement du connecteur S3...'
curl -X POST http://kafka-connect:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @/config.json

echo ''
echo '✅ Connecteur déployé'

echo '📋 Vérification finale:'
sleep 3
curl -s http://kafka-connect:8083/connectors/s3-sink-taasim/status
echo ''
echo '🎉 Initialisation terminée !'
