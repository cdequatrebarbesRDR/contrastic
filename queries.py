#!/usr/bin/env/python
# -*- coding: utf-8 -*-
#   
from config import settings
from database import select_one, select_many
import datetime
from typing import List, Union

def get_pers_id_from_id(id:int)-> int:
    '''PERS_ID = NUMERO_ADHERENT *100 + RANG 0 pour AP
    PERS_ID = NUMERO_ADHERENT *100 + RANG > 0 pour Beneficiaire
    '''
    return id*100

def filter_contrats(contrats, filter_key="mutuelle"):
    """
    Fonction permettant de filtrer les sous elements d'un contrats en fonction 
    d'une clé de filtre contenue dans le contrat et de retourner une liste de valeurs uniques
    """
    if contrats is None:
        return None
    if filter_key is None:
        return contrats
    seen = set()
    uniquelist = []
    for c in contrats:
        if c is not None and filter_key in c and c[filter_key] is not None:
            if c[filter_key]["id"] not in seen:
                seen.add(c[filter_key]["id"])
                uniquelist.append(c[filter_key])
    return uniquelist

def get_distinct_adhe_ids() -> list:
    query = f"SELECT DISTINCT PERS_ID FROM {settings.DB_NAME}.DTWPERS"
    adhe_ids = select_many(query)
    return set(adhe_ids)

def get_distinct_entre_ids() -> list:
    query = f"SELECT DISTINCT ENTR_ID FROM {settings.DB_NAME}.DTWENTR"
    entre_ids = select_many(query)
    return set(entre_ids)

def get_common_ids():
    adh_ids = get_distinct_adhe_ids()
    entr_ids = get_distinct_entre_ids()
    return adh_ids, entr_ids, list(adh_ids.intersection(entr_ids))

# def valid_contrat(contrat):
#     return bool(contrat is not None and contrat["statut"] == "ACT" and contrat["date_fin"] >= datetime.date.today())
    
# def get_contrat_ids_from_uid(uid):
#     return [].extend(get_pers_contrat_ids(uid))

# def get_common_users_actifs():
#     adh_ids = get_distinct_adhe_ids()
#     entr_ids = get_distinct_entre_ids()
#     total = len(adh_ids) +  len(entr_ids)

#     common_uids = list(adh_ids.intersection(entr_ids))
#     common = len(common_uids)
#     actif_uids = set()
#     for uid in common_uids:
#         entr_contrats_ids = get_entr_contrat_ids(uid)
#         if entr_contrats_ids is not None:
#             entr_active = any([select_contrat(cid) is not None for cid in entr_contrats_ids if cid is not None])
#         else:
#             entr_active = False
#         pers_contrats_ids = get_pers_contrat_ids(uid)
#         if pers_contrats_ids is not None :
#             pers_active = any([select_contrat(cid) is not None for cid in pers_contrats_ids if cid is not None])
#         else:
#             pers_active = False

#         if pers_active and entr_active:
#             actif_uids.add(uid)
#     common_actif = len(actif_uids)
#     ratio = common_actif/total    
#     return (common_actif, actif_uids, ratio, total, common)

# def is_contrat_valid(cntr):
#     print(cntr["date_debut"], cntr["date_fin"])




def get_pers_contrat_ids(pers_id: int) -> list| None:
    """
    Fonction permettant de récupérer les numéros de contrats à partir d'un code assuré
    """
    query = f"SELECT ADHE_CNTR_ID FROM {settings.DB_NAME}.DTWADHE AS ADH WHERE ADH.ADHE_PERS_ID = {str(pers_id)}"
    return select_many(query)
    
    
def get_entr_contrat_ids(entr_id: int) -> list| None:
    """
    Fonction permettant de récupérer les numéros de contrats à partir d'un code entreprise
    """
    query = f"SELECT CNTR_ID FROM {settings.DB_NAME}.DTWCNTR AS CNTR WHERE CNTR.CNTR_ENTR_ID = {str(entr_id)}"
    return select_many(query)
    

def get_entr_ids() -> list| None:
    query = f"SELECT DISTINCT CNTR_ENTR_ID FROM {settings.DB_NAME}.DTWCNTR"
    cntr_ids = select_many(query)
    return set(cntr_ids)

def get_pers_ids() -> list| None:
    query = f"SELECT DISTINCT ADHE_PERS_ID FROM {settings.DB_NAME}.DTWADHE"
    pers_ids = select_many(query)
    return set(pers_ids)

def select_entreprise(id: int) -> dict| None:
    """
    Fonction permettant de récupérer les informations d'une entreprise à partir de son identifiant
    """
    query = f"SELECT * FROM {settings.DB_NAME}.DTWENTR AS ENT WHERE ENT.ENTR_ID = {str(id)}"
    header = ["id", "raison_sociale","siret","code_ape","code_naf","fjur","rue_ligne1","rue_ligne2","rue_ligne3","dpt","cp","ville","pays","tel","centre_gest","atc","charge_cpte","date_creation","date_ins","date_modif"]
    entreprise_d = select_one(query, header)
    
    if entreprise_d is None:
        return None
    return entreprise_d

def select_opta_codes(contrat_id:int) -> dict| None:
    """
    Fonction permettant de récupérer les codes d'options à partir d'un numéro de contrat
    """
    query = f"SELECT DETAIL.DECO_OPTA_CODE DOC.OPTA_LIBELLE DOC.OPTA_NUM_CRIT FROM {settings.DB_NAME}.DTWDECO AS DETAIL  {settings.DB_NAME}.DTWOPTA AS DOC INNER JOIN ON DETAIL.DECO_OPTA_CODE = DOC.OPTA_CODE WHERE DETAIL.DECO_CNTR_ID={str(contrat_id)} "
    return list(set(select_many(query, ["code", "libelle", "numero_critere"])))      



def select_contrat(contrat_id:int, contrat_type:str="personne-physique") -> dict| None:
    query = f"SELECT * FROM {settings.DB_NAME}.DTWCNTR AS CNTR  INNER JOIN {settings.DB_NAME}.DTWDECO AS DECO ON CNTR.CNTR_ID = DECO.DECO_CNTR_ID WHERE CNTR.CNTR_ID = {str(contrat_id)}"
    header1 = ["id","entr_id","agen_id","catg_code","prdt_id","date_debut","date_fin","date_suspension","motif_debut","motif_fin","num_charge_cpte","charge_compte","statut","responsable","terme_appel","ordre_decptage","assistance","mode_paiement","impaye_per","impaye_mnt","date_mise_dem","exo_coti","date_modif_erp","date_arrete","date_ins","date_modif"]
    header2 = ["detail_id","cntr_id","adhe_id","opta_code","risque","ss_risque","branche","source","type","statut","num_met","police","police_edi","cg","date_ins","date_modif"]
    contrat = select_one(query, header1 + header2)
    if contrat is not None:
        if contrat_type == "personne-physique":
            contrat["entreprise"] = select_entreprise(contrat["entr_id"])
            
        del contrat["cntr_id"]
        del contrat["entr_id"]
        del contrat["detail_id"]
        if contrat["impaye_mnt"] is not None:
            contrat["impaye_mnt"] = float(contrat["impaye_mnt"])
        contrat["produit"] = select_produit(int(contrat["prdt_id"]))
        contrat["garantie"] = select_garantie(int(contrat["prdt_id"]))
        if contrat["garantie"] is not None:
            contrat["compagnie"] = select_compagnie(contrat["garantie"]["compagnie_id"])
        del contrat["prdt_id"]
        contrat["categorie"] = select_categorie(contrat["catg_code"])
        del contrat["catg_code"]
        contrat["mutuelle"] = select_mutuelle(contrat["agen_id"])
        del contrat["agen_id"]
        contrat.update({"options_codes": select_opta_codes(contrat_id)})
    return contrat

def select_mutuelles()-> List[dict]| None:
    query = f"SELECT * FROM {settings.DB_NAME}.DTWAGEN "
    header1 = ["id","libelle","portefeuille", "marque"]
    mutuelles = select_many(query, header1)
    if mutuelles is not None:
        muts = []
        for row in mutuelles:
            portefeuille_id = int(row["portefeuille"][1:])
            if portefeuille_id < 51:
                row["marque"] = "ROEDERER"
            elif portefeuille_id < 60:
                row["marque"] = "SIMAX"
            else:
                row["marque"] = "EOLE"
            muts.append(row)
        return [Mutuelle(**m) for m in muts]
    return mutuelles

def select_mutuelle(agen_id)-> dict|None:
    query = f"SELECT * FROM {settings.DB_NAME}.DTWAGEN WHERE AGEN_ID = {str(agen_id)}"
    header1 = ["id","libelle","portefeuille", "marque"]
    row = select_one(query, header1)
    if row is not None:
        portefeuille_id = int(row["portefeuille"][1:])
        if portefeuille_id < 51:
            row["marque"] = "ROEDERER"
        elif portefeuille_id < 60:
            row["marque"] = "SIMAX"
        else:
            row["marque"] = "EOLE"
        return row
    return row

def select_produits():
    query = f"SELECT * FROM {settings.DB_NAME}.DTWPRDT AS PRDT"
    header1 = ["id","nom","option","libelle","libelle_court","code_famille","famille","type_gamme","date_ins","date_modif"]
    produits = select_many(query, header1)
    return produits

def select_produit(prdt_id:int):    
    query = f"SELECT * FROM {settings.DB_NAME}.DTWPRDT AS PRDT WHERE PRDT.PRDT_ID = {str(prdt_id)}"
    header1 = ["id","nom","option","libelle","libelle_court","code_famille","famille","type_gamme","date_ins","date_modif"]
    produit = select_one(query, header1)
    return produit

def select_categories():
    query = f"SELECT * FROM {settings.DB_NAME}.DTWCATG"
    header1 = ["code","libelle","grd_college","famille_coti"]
    categories = select_many(query, header1)
    return categories

def select_categorie(catg_code:str):
    query = f"SELECT * FROM {settings.DB_NAME}.DTWCATG AS CAT WHERE CAT.CATG_CODE = '{catg_code}'"
    header1 = ["id","libelle","grd_college","famille_coti"]
    categorie = select_one(query, header1)  
    return categorie

def select_garanties():
    query = f"SELECT * FROM {settings.DB_NAME}.DTWGTIE AS GTIE"
    header1 = ["id","compagnie","risque","sous_rique", "branche"]
    garanties = select_many(query, header1)
    return garanties

def select_garantie(prdt_id:int):
    query = f"SELECT * FROM {settings.DB_NAME}.DTWGTIE AS GTIE INNER JOIN {settings.DB_NAME}.DTWPRGA AS PRD ON PRD.PRGA_GARA_ID=GTIE.GARA_ID WHERE PRD.PRGA_PRDT_ID = '{str(prdt_id)}'"
    header1 = ["id","compagnie_id","risque","sous_rique", "branche"]
    garantie = select_one(query, header1)
    return garantie  

def select_compagnies() -> List[dict]| None:
    query = f"SELECT * FROM {settings.DB_NAME}.DTWCIE AS CIE"
    header1 = ["id","libelle",]
    compagnies = select_many(query, header1)
    return compagnies

def select_compagnie(compagnie_id:int) -> dict | None:    
    query = f"SELECT * FROM {settings.DB_NAME}.DTWCIE AS CIE WHERE CIE.COMP_ID = '{str(compagnie_id)}'"
    header1 = ["id","libelle"]
    cie = select_one(query, header1)
    return cie

def select_services(pers_id:int,contrat_type:str="personne-physique") -> List[dict]|None:
    if contrat_type == "personne-physique":
        #{str(id*100+0)}
        query = f"SELECT PERS_EBIA_ACTIF FROM {settings.DB_NAME}.DTWPERS AS PERS WHERE PERS.PERS_ID = {str(pers_id)}"
        row = select_one(query)
        if row is not None and len(row) > 0: 
            return [{"name":"EBIA", "actif": row[0] is not None and row[0]== 'O'}]
    else:
        query = f"SELECT PREN_PROPRIETE,PREN_VALEUR FROM {settings.DB_NAME}.DTWPREN AS ENTR WHERE ENTR.PREN_ENTR_ID = {str(pers_id)}"
        rows = select_many(query, ["service", "value"])
        if rows is not None and len(rows) > 0: 
            services = []
            for row in rows: 
                services.append({'name': row["service"], "actif": row["value"] is not None and row["value"] == 'O'})
            return services
    return None

def select_assure(pers_id:int) -> dict| None:
    """
    Fonction permettant de récupérer les informations d'une personne physique à partir de son identifiant
    """
    if len(str(pers_id)) <= 6:
        pers_id = get_pers_id_from_id(pers_id)
        code = 0
    else:
        code = int(str(pers_id)[-2:])


    query = f"SELECT * FROM {settings.DB_NAME}.DTWPERS AS PERS WHERE PERS.PERS_ID = {str(pers_id)}"
    header1 = ["id","numero","matricule","numero_ss","nom","prenom","patronyme","date_naiss","regime","grand_regime","sexe","type","rang_jumeau","situation_fam","teletrans","alert_mail","date_cpte_cli","rue_ligne1","rue_ligne2","rue_ligne3","cp","ville","pays","date_adr","email","tel_fixe","tel_mobile","parent_id","date_creation","aut_prelev","EBIA", "date_ins","date_modif"]
    assure = select_one(query, header1)
    if assure is not None:
        assure["rang"] = code
        assure["services"] = [{"name":"EBIA", "actif":assure["EBIA"] is not None and assure["EBIA"]=='O'}]
        del assure["EBIA"]
    return assure
    
def select_personne_physique(id:int) -> dict | None:
    """
    Fonction permettant de récupérer les informations d'une personne physique à partir de son identifiant
    """
    # query = f"SELECT * FROM {settings.DB_NAME}.DTWPERS AS PERS WHERE PERS.PERS_ID = {str(id*100+0)}"
    # header1 = ["id","nom","prenom","date_naissance","sexe","adresse_ligne1","adresse_ligne2","adresse_ligne3","dpt","cp","ville","pays","tel_fixe","tel_mobile"]
    # personne_physique = select_one(query, header1)
    personne_physique = select_assure(id)
    if personne_physique is not None:
        #personne_physique["services"] = [Service(name="EBIA", actif=personne_physique["EBIA"] is not None and personne_physique["EBIA"]=='O')]
        contrat_ids = get_pers_contrat_ids(personne_physique["id"])
        if contrat_ids is None or len(contrat_ids) == 0:
            return personne_physique
        contrats = [select_contrat(cntr_id) for cntr_id in contrat_ids]
        contrats = [c for c in contrats if c is not None and c["statut"] == "ACT"]
        if len(contrats) > 0:
            personne_physique["contrats"] = contrats
            personne_physique["beneficiaires"] = select_beneficiaires(personne_physique['numero'])
            #personne_physique["services"] = select_services(personne_physique["id"], "personne-physique")
            personne_physique["mutuelles"] = filter_contrats(contrats, "mutuelle")
            personne_physique["produits"] = filter_contrats(contrats, "produit")
            personne_physique["garanties"] =  filter_contrats(contrats, "garantie")
            personne_physique["compagnies"] = filter_contrats(contrats, "compagnie")
            personne_physique["categories"] = filter_contrats(contrats, "categorie")
            personne_physique["entreprises"] = filter_contrats(contrats, "entreprise")
            return personne_physique
    return personne_physique

def select_personne_morale(id:int) -> dict| None:
    """
    Fonction permettant de récupérer les informations d'une personne morale à partir de son identifiant
    """     
    query = f"SELECT * FROM {settings.DB_NAME}.DTWENTR AS ENT WHERE ENT.ENTR_ID = {str(id)}"
    header1 = ["id","raison_sociale","siret","code_ape","code_naf","fjur","rue_ligne1","rue_ligne2","rue_ligne3","dpt","cp","ville","pays","tel","centre_gest","atc","charge_cpte","date_creation","date_ins","date_modif"]
    personne_morale = select_one(query, header1)
    print("pers")
    if personne_morale is not None:
        personne_morale["services"] = select_services(personne_morale["id"], "personne-morale")
        contrat_ids = get_entr_contrat_ids(personne_morale["id"])
        if contrat_ids is not None and len(contrat_ids) > 0:
            contrats = [select_contrat(cntr_id, "personne-morale") for cntr_id in contrat_ids]
            personne_morale["contrats"] = contrats
            personne_morale["mutuelles"] = filter_contrats(contrats, "mutuelle")
            personne_morale["produits"] = filter_contrats(contrats, "produit")
            personne_morale["garanties"] =  filter_contrats(contrats, "garantie")
            personne_morale["compagnies"] = filter_contrats(contrats, "compagnie")
            return personne_morale
        
    return personne_morale

def select_beneficiaires(id:int) -> dict | None:
    """
    Fonction permettant de récupérer les informations d'un bénéficiaire à partir de son identifiant
    """
    query = f"SELECT PERS_ID FROM {settings.DB_NAME}.DTWPERS AS PERS WHERE PERS.PERS_NUMERO = {str(id)}"
    assure_ids = select_many(query)
    if assure_ids is not None:
        return [select_assure(a_id) for a_id in assure_ids]
    return assure_ids 

