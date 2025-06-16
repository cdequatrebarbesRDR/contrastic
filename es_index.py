from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv(./elastic-start-local/.env)
# Initialize the Elasticsearch client with the API key  # and the URL of the Elasticsearch instance
client = Elasticsearch(
    os.environ.get("ES_LOCAL_URL", "http://localhost:9200"),
    api_key=os.environ.get("ES_LOCAL_API_KEY"),
)


mappings = {
    "properties": {
        "_id": {"type": "integer"},
        "type": {"type": "text"},
        "date_debut": {"type": "date", "format": "yyyy-MM-dd"},
        "date_fin": {"type": "date", "format": "yyyy-MM-dd"},
        "date_creation": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},#date_insertion
        "date_modification": {"type": "date", "format": "yyyy-MM-dd  HH:mm:ss"},#date_modif
        "date_suspension": {"type": "date", "format": "yyyy-MM-dd"},#date_suspension
        "motif": {
            "properties": {
                "debut":{
                    "type": "text"
                },
                "fin": {
                    "type": "text"
                }
            }
        }
        "status": {
            "type": "text"
        },
        "terme_appel" :{
            "type": "text"
        },
        "ordre_decryptage" :{
                "type": "text"
            },
        "assistance" :{
                "type": "text"
            },
        "mode_paiement" :{
                "type": "text"
            },
        "impaye_periode" :{
                "type": "text"
            },
        "impaye_montant" : {
            "type": "float"
        },
        "date_mise_en_demeure": {
            "type":   "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
        }
        "exo_cotisation" :{
                "type": "text"
            },
        "date_modif_erp": {
            "type":   "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
        }
        "date_arrete":{
                "type":   "date",
                "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
        }
        "created_at" :{
            "type":   "date",
        }
        "updated_at" :{
            "type":   "date",
            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
        },
        "charge_compte": {
            "properties": {
                "id":  { "type": "integer" },
                "nom": { "type": "text" },
                "prenom": { "type": "text" },

            }
        },
        "mutuelle":{
            "properties": {
                "id":  { "type": "integer" },
                "portefeuille": { "type": "text" },
                "libelle": { "type": "text" }
            }

        }
        "categorie": {
            "properties": {
                "code": {"type": "text"},
                "libelle": {"type": "text"},
                "grand_college": {"type": "integer"},
                "description": {"type": "text"},
            }
        },
        # "entreprise": {
        #     "properties": {
        #         "id": {"type": "integer"},
        #         "nom": {"type": "text"},
        #         "prenom": {"type": "text"},
        #         "email": {"type": "text"},
        #         "telephone": {"type": "text"},
        #     }
        # },
        # "assure": {
        #     "properties": {
        #         "id": {"type": "integer"},
        #         "nom": {"type": "text"},
        #         "prenom": {"type": "text"},
        #         "email": {"type": "text"},
        #         "telephone": {"type": "text"},
        #     }
        # },
        "produit": {
            "properties": {
                "id": {"type": "integer"},
                "nom": {"type": "text"},
                "code": {"type": "text"},
                "description": {"type": "text"},
                "options": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "integer"},
                        "nom": {"type": "text"},
                        "code": {"type": "text"},
                        "description": {"type": "text"},
                    }
                }
            }
        }
        "adhe_id","opta_code","risque","ss_risque","branche","source","type","statut","num_met","police","police_edi","cg","date_ins","date_modif"
        "mutuelle"  : {"type": "text"},
        "produit": {"type": "text"},
    }
}

client.indices.create(index="contrats", mappings=mappings, ignore=400)
