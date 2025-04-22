from confluent_kafka import Producer
from flask import Flask, jsonify
from datetime import datetime
import requests
import logging
import json
import os

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

TOPIC = "explicit_energy_tracks"  # Cambiado el nombre del tópico

JSON_URL = "https://raw.githubusercontent.com/AntonioIR1218/spark-labs/refs/heads/main/results/explicit_energy_tracks.json"

# Credenciales actualizadas
producer_conf = {
    'bootstrap.servers': 'd03h267mtrpq60sg5cf0.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'tony',
    'sasl.password': '1234'
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err:
        logging.error(f'Error al enviar: {err}')
    else:
        logging.info(f'Enviado al tópico {msg.topic()}: {msg.value().decode("utf-8")}')

def transform_track_data(track):
    """Transforma los datos del track para MongoDB"""
    try:
        return {
            '_id': track.get('id', str(datetime.now().timestamp())),
            'name': track.get('name', ''),
            'artists': track.get('artists', []),
            'energy': track.get('energy', 0),
            'explicit': track.get('explicit', False),
            'duration_ms': track.get('duration_ms', 0),
            'popularity': track.get('popularity', 0),
            'metadata': {
                'source': 'Spotify',
                'imported_at': datetime.utcnow(),
                'dataset': 'explicit_energy_tracks'
            }
        }
    except Exception as e:
        logging.warning(f"Error transformando datos: {e}")
        return None

def fetch_and_send_data():
    response = requests.get(JSON_URL)
    response.raise_for_status()
    
    # Obtenemos la lista de strings JSON
    tracks_raw = response.json()
    
    # Convertimos cada string en un diccionario
    try:
        tracks = [json.loads(t) for t in tracks_raw]
    except json.JSONDecodeError as e:
        logging.error(f"Error decodificando JSON: {e}")
        return 0, len(tracks_raw), len(tracks_raw)

    logging.info(f"Tracks recibidos: {len(tracks)}")

    success, failed = 0, 0

    for track in tracks:
        try:
            message = transform_track_data(track)

            if message:
                producer.produce(
                    topic=TOPIC,
                    value=json.dumps(message, default=str).encode('utf-8'),
                    callback=delivery_report
                )
                success += 1
            else:
                failed += 1
        except Exception as e:
            logging.warning(f"Error procesando track: {e}")
            failed += 1

    producer.flush()
    return success, failed, len(tracks)

@app.route('/send-tracks', methods=['POST'])
def send_track_data():
    try:
        success, failed, total = fetch_and_send_data()
        logging.info(f"Éxitos: {success} | Fallos: {failed}")

        return jsonify({
            "status": "success",
            "message": f"Datos enviados al tópico '{TOPIC}'",
            "stats": {
                "total": total,
                "success": success,
                "failed": failed
            }
        }), 200

    except Exception as e:
        logging.error(f"Error general: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
