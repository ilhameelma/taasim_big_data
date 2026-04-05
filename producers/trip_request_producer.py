#!/usr/bin/env python3
"""
TaaSim - Trip Request Producer
Selon specs du projet: page 19 - Simule des réservations citoyens

Utilise les courbes de demande générées par le notebook:
- demand_curve_hourly.json: demande par heure (normalisée entre 0 et 1)
- demand_curve_daily.json: demande par jour de semaine (normalisée entre 0 et 1)
"""

import json
import time
import random
import uuid
import argparse
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TripRequestProducer:
    def __init__(self, bootstrap_servers='kafka:9092', data_path=None, speed_factor=1.0):
        self.bootstrap_servers = bootstrap_servers
        self.speed_factor = speed_factor
        self.data_path = Path(data_path) if data_path else Path(__file__).parent.parent / 'data'
        
        # Charger les courbes de demande
        self.demand_hourly = {}
        self.demand_daily = {}
        self.load_demand_curves()
        
        # Charger le mapping des zones
        self.load_zone_mapping()
        
        # Producer Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.sent_count = 0
        
    def load_demand_curves(self):
        """Charge les courbes de demande depuis les fichiers JSON"""
        # Charger la courbe horaire
        hourly_path = self.data_path / 'demand_curve_hourly.json'
        if hourly_path.exists():
            with open(hourly_path, 'r') as f:
                self.demand_hourly = json.load(f)
            logger.info(f"Courbe horaire chargée: {len(self.demand_hourly)} heures")
            # Convertir les clés en int si nécessaire
            self.demand_hourly = {int(k): v for k, v in self.demand_hourly.items()}
        else:
            logger.warning(f"demand_curve_hourly.json non trouvé, utilisation des valeurs par défaut")
            self.demand_hourly = {
                0: 0.3, 1: 0.2, 2: 0.1, 3: 0.1, 4: 0.2, 5: 0.5,
                6: 0.8, 7: 3.5, 8: 4.0, 9: 3.0, 10: 1.5, 11: 1.8,
                12: 1.5, 13: 1.2, 14: 1.2, 15: 1.5, 16: 2.0,
                17: 3.5, 18: 4.0, 19: 3.0, 20: 1.8, 21: 1.5,
                22: 1.0, 23: 0.6
            }
        
        # Charger la courbe journalière
        daily_path = self.data_path / 'demand_curve_daily.json'
        if daily_path.exists():
            with open(daily_path, 'r') as f:
                self.demand_daily = json.load(f)
            logger.info(f"Courbe journalière chargée: {len(self.demand_daily)} jours")
            # Convertir les clés en int si nécessaire
            self.demand_daily = {int(k): v for k, v in self.demand_daily.items()}
        else:
            logger.warning(f"demand_curve_daily.json non trouvé, utilisation des valeurs par défaut")
            # 0=Lundi, 1=Mardi, 2=Mercredi, 3=Jeudi, 4=Vendredi, 5=Samedi, 6=Dimanche
            self.demand_daily = {
                0: 0.83, 1: 0.85, 2: 0.84, 3: 0.89, 4: 1.0, 5: 0.93, 6: 0.83
            }
        
        # Normaliser les valeurs pour qu'elles soient entre 0 et 1
        max_hourly = max(self.demand_hourly.values())
        self.demand_hourly = {h: v / max_hourly for h, v in self.demand_hourly.items()}
        
        max_daily = max(self.demand_daily.values())
        self.demand_daily = {d: v / max_daily for d, v in self.demand_daily.items()}
        
        logger.info("Courbes de demande normalisées (valeurs entre 0 et 1)")
        
    def load_zone_mapping(self):
        """Charge la table des zones Casablanca avec toutes les nouvelles colonnes"""
        zone_path = self.data_path / 'zone_mapping.csv'
        if zone_path.exists():
            self.zones_df = pd.read_csv(zone_path)
            # Utiliser toutes les colonnes disponibles
            self.zones = self.zones_df.to_dict('records')
            logger.info(f"Zones chargées: {len(self.zones)}")
            logger.info(f"Colonnes disponibles: {list(self.zones_df.columns)}")
            
            # Afficher quelques statistiques sur les zones
            if 'population_density' in self.zones_df.columns:
                # Compter les valeurs uniques de densité
                densities = self.zones_df['population_density'].unique()
                logger.info(f"Population density values: {list(densities)}")
            if 'base_fare_mad' in self.zones_df.columns:
                logger.info(f"Base fare - min: {self.zones_df['base_fare_mad'].min()} MAD, max: {self.zones_df['base_fare_mad'].max()} MAD")
        else:
            logger.warning(f"zone_mapping.csv non trouvé dans {zone_path}")
            self.zones = [{'zone_id': i, 'zone_type': 'mixed'} for i in range(1, 17)]
    
    def get_demand_multiplier(self, current_time):
        """
        Calcule le multiplicateur de demande basé sur:
        - L'heure de la journée (demand_curve_hourly.json)
        - Le jour de la semaine (demand_curve_daily.json)
        
        Returns:
            float: multiplicateur de demande (entre 0 et 1)
        """
        hour = current_time.hour
        # Python: 0=Lundi, 1=Mardi, ..., 6=Dimanche
        # Notre fichier daily.json utilise 0=Lundi, 6=Dimanche (même convention)
        day_of_week = current_time.weekday()
        
        # Obtenir les facteurs
        hourly_factor = self.demand_hourly.get(hour, 0.5)
        daily_factor = self.demand_daily.get(day_of_week, 0.8)
        
        # Combiner les deux facteurs (moyenne pondérée)
        # On donne plus de poids à l'heure qu'au jour
        combined_factor = (hourly_factor * 0.7 + daily_factor * 0.3)
        
        return combined_factor
    
    def convert_density_to_score(self, density_value):
        """
        Convertit la densité de population textuelle en score numérique
        
        Args:
            density_value: 'high', 'medium', 'low' ou None
            
        Returns:
            float: score entre 0.5 et 2.0
        """
        if density_value is None or pd.isna(density_value):
            return 1.0
        
        density_str = str(density_value).lower()
        
        density_scores = {
            'high': 1.5,
            'medium': 1.0,
            'low': 0.6,
            'very high': 1.8,
            'very low': 0.4
        }
        
        return density_scores.get(density_str, 1.0)
    
    def get_zone_weight(self, zone):
        """
        Calcule le poids d'une zone pour la sélection (zones plus denses = plus de demandes)
        
        Args:
            zone: dictionnaire contenant les informations de la zone
            
        Returns:
            float: poids de la zone (plus élevé = plus de chance d'être sélectionnée)
        """
        # Base weight = 1
        weight = 1.0
        
        # Ajuster selon la densité de population (maintenant gère les strings)
        if 'population_density' in zone and zone['population_density'] is not None:
            weight *= self.convert_density_to_score(zone['population_density'])
        
        # Ajuster selon le type de zone
        if 'zone_type' in zone:
            zone_type = str(zone['zone_type']).lower()
            if zone_type == 'commercial':
                weight *= 1.3  # Zones commerciales plus actives
            elif zone_type == 'residential':
                weight *= 0.8  # Zones résidentielles moins actives
            elif zone_type == 'industrial':
                weight *= 0.6  # Zones industrielles peu actives
            elif zone_type == 'transit_hub':
                weight *= 1.5  # Hubs de transport très actifs
        
        return max(0.3, min(weight, 3.0))  # Limiter entre 0.3 et 3.0
    
    def select_weighted_zone(self, exclude_zone=None):
        """
        Sélectionne une zone aléatoire avec un poids basé sur sa densité et son type
        
        Args:
            exclude_zone: ID de zone à exclure (pour éviter origine = destination)
            
        Returns:
            dict: zone sélectionnée
        """
        # Calculer les poids pour chaque zone
        eligible_zones = []
        weights = []
        
        for zone in self.zones:
            if exclude_zone is not None and zone['zone_id'] == exclude_zone:
                continue
            eligible_zones.append(zone)
            weights.append(self.get_zone_weight(zone))
        
        # Sélectionner avec les poids
        selected = random.choices(eligible_zones, weights=weights, k=1)[0]
        return selected
    
    def get_call_type(self, hour):
        """
        Distribution CALL_TYPE basée sur l'heure
        - A (dispatché): plus fréquent aux heures de pointe
        - B (station): constant
        - C (rue): plus fréquent le soir et le week-end
        """
        # Ajuster les probabilités selon l'heure
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            # Heures de pointe: plus de dispatchés
            weights = [0.4, 0.3, 0.3]  # A, B, C
        elif 20 <= hour <= 23:
            # Soir: plus de rue
            weights = [0.2, 0.3, 0.5]
        elif 0 <= hour <= 5:
            # Nuit: plus de station
            weights = [0.2, 0.5, 0.3]
        else:
            # Jour normal: distribution standard
            weights = [0.3, 0.3, 0.4]
        
        return random.choices(['A', 'B', 'C'], weights=weights)[0]
    
    def create_trip_request(self, current_time):
        """Crée un message de réservation selon specs page 19"""
        # Calculer le multiplicateur de demande
        demand_multiplier = self.get_demand_multiplier(current_time)
        
        # Base rate = 1 réservation par seconde au max
        base_rate = demand_multiplier * self.speed_factor
        
        # Sélectionner les zones avec pondération
        origin_zone = self.select_weighted_zone()
        destination_zone = self.select_weighted_zone(exclude_zone=origin_zone['zone_id'])
        
        # Calculer le prix estimé (si les données sont disponibles)
        estimated_fare = None
        if 'base_fare_mad' in origin_zone and 'base_fare_mad' in destination_zone:
            try:
                base_fare = (float(origin_zone.get('base_fare_mad', 5)) + 
                            float(destination_zone.get('base_fare_mad', 5))) / 2
                # Ajouter un facteur aléatoire
                estimated_fare = round(base_fare + random.uniform(-2, 5), 2)
                estimated_fare = max(5, estimated_fare)  # Minimum 5 MAD
            except (ValueError, TypeError):
                estimated_fare = 8.0  # Valeur par défaut
        
        # Créer le message
        request = {
            "trip_id": str(uuid.uuid4()),
            "rider_id": f"rider_{random.randint(1, 10000)}",
            "origin_zone": origin_zone['zone_id'],
            "origin_zone_name": origin_zone.get('zone_name', 'Unknown'),
            "origin_zone_type": origin_zone.get('zone_type', 'mixed'),
            "destination_zone": destination_zone['zone_id'],
            "destination_zone_name": destination_zone.get('zone_name', 'Unknown'),
            "destination_zone_type": destination_zone.get('zone_type', 'mixed'),
            "requested_at": current_time.isoformat(),
            "call_type": self.get_call_type(current_time.hour),
            "passenger_count": random.randint(1, 4),
            "demand_multiplier": round(demand_multiplier, 3)
        }
        
        # Ajouter le prix estimé si disponible
        if estimated_fare:
            request["estimated_fare_mad"] = estimated_fare
        
        # Ajouter la densité de population si disponible (convertie en score)
        if 'population_density' in origin_zone:
            density_score = self.convert_density_to_score(origin_zone.get('population_density'))
            request["origin_density_score"] = density_score
        
        return request, base_rate
    
    def run(self, duration_seconds=None):
        """Lance la simulation (page 19)"""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=duration_seconds) if duration_seconds else None
        
        logger.info("=" * 60)
        logger.info("🚕 Démarrage du producer de réservations TaaSim")
        logger.info(f"📊 Speed factor: {self.speed_factor}x")
        logger.info(f"📍 Courbe de demande: horaire + journalière")
        logger.info(f"🗺️ Zones disponibles: {len(self.zones)}")
        logger.info("=" * 60)
        
        try:
            while True:
                current_time = datetime.now()
                
                if end_time and current_time >= end_time:
                    break
                
                # Créer et envoyer un message
                request, base_rate = self.create_trip_request(current_time)
                
                # Ajuster le délai selon la demande
                # base_rate est le nombre de réservations par seconde
                delay = 1.0 / max(base_rate, 0.1)
                
                self.producer.send('raw.trips', request)
                self.sent_count += 1
                
                if self.sent_count % 100 == 0:
                    logger.info(f"📊 Envoyé: {self.sent_count} réservations | Taux: {base_rate:.2f} req/s")
                
                time.sleep(delay / self.speed_factor)
                
        except KeyboardInterrupt:
            logger.info("🛑 Arrêt demandé par l'utilisateur")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"✅ Total envoyé: {self.sent_count} réservations")
            logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Trip Request Producer pour TaaSim',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  # Simulation normale (1x)
  python trip_request_producer.py --speed-factor 1.0
  
  # Simulation accélérée (5x)
  python trip_request_producer.py --speed-factor 5.0
  
  # Simulation pendant 60 secondes
  python trip_request_producer.py --duration 60 --speed-factor 2.0
  
  # Avec données personnalisées
  python trip_request_producer.py --data-path /home/jovyan/work/data --speed-factor 3.0
        """
    )
    
    parser.add_argument('--bootstrap-servers', default='kafka:9092',
                        help='Adresse du broker Kafka')
    parser.add_argument('--data-path', default=None,
                        help='Chemin vers les données (contient zone_mapping.csv et demand_curve_*.json)')
    parser.add_argument('--speed-factor', type=float, default=1.0,
                        help='Facteur d\'accélération (défaut: 1.0 = temps réel)')
    parser.add_argument('--duration', type=int, default=None,
                        help='Durée de simulation en secondes (défaut: infini)')
    
    args = parser.parse_args()
    
    producer = TripRequestProducer(
        bootstrap_servers=args.bootstrap_servers,
        data_path=args.data_path,
        speed_factor=args.speed_factor
    )
    
    producer.run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()