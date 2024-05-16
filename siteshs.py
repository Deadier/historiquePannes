#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Auteurs: Romain MAZIERE and Gaspard FEREY (Arcep)
#
# Processus de récupération des sites indisponibles de France métropolitaine
# publiés par les opérateurs et sauvegarde des données uniformisées
# aux formats CSV, JSON et GeoJSON.
# 
# Nécessite Python 3.7

import sys, time, re, json, requests
from datetime import date, datetime
import numpy as np
import pandas as pd
import geopandas as gpd

from operators import *
from paths import PathHandler

# Si on fournit un 5ème argument (date), utiliser les fichiers préalablement sauvegardés
# Sinon télécharger les fichiers à la date d'aujourd'hui
from_download = len(sys.argv) < 3

# La date sur laquelle tourner
datename = str(date.today()) if from_download else sys.argv[2]

print("")
print("################################################")
print("    Lancement du script à la date du ", datename)
print("################################################")
print("")

# Chemin vers le dossier de sauvegarde
save = PathHandler(sys.argv[1], datename)

def try_download(op):
    """ Tentative de téléchargement du fichier opérateur. Renvoie True en cas de succès, False sinon. """
    try:
        r = requests.get(op['url'], allow_redirects=True, timeout=10)
        if r.status_code != 200:
            return False
        else:
            print("Fichier téléchargé.")
            # sauvegarde sur le disque
            export_file = save.raw_path(op, datename)
            with open(export_file, 'wb') as file:
                file.write(r.content)
            print("Sauvegardé à " + export_file)
            return True
    except Exception as e:
        print(f"Erreur lors du téléchargement: {e}")
        return False

def download(op, maxtry):
    """ Effectue maxtry tentatives de téléchargement du fichier opérateur. """
    for i in range(maxtry):
        print("Tentative :", i + 1)
        if try_download(op):
            print("Succès du téléchargement !")
            return True
        else:
            print("Echec de téléchargement !")
            time.sleep(5) # 5 secondes de politesse entre deux tentatives

# Téléchargement des fichiers opérateur si nécessaire
if from_download:
    for op in operateurs:
        print("Téléchargement de " + op['name'] + " : " + op['url'])
        download(op, 5)

def get_raw_dataframe(op):
    """ Fonction de récupération d'un dataframe brut à partir des fichiers récupérés """
    try:
        if op["type"] == "xls":
            return pd.read_excel(save.raw_path(op, datename),
                                 sheet_name=op['excelsheet'],
                                 header=op['excelheader'],
                                 index_col=None)
        else:
            return pd.read_csv(save.raw_path(op, datename),
                               sep=op['separator'],
                               skiprows=op['skipheader'],
                               skipfooter=op['skipfooter'],
                               encoding=op.get('encoding'),
                               engine='python')
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier pour {op['name']}: {e}")
        return pd.DataFrame()  # Retourne un dataframe vide en cas d'erreur

# Calcul des champs lat/long à partir des x/y en Lambert93
def coords_conversion(df):
    try:
        # Reprojection en WGS84 des coordonées projetées
        pt = gpd.GeoDataFrame(geometry=gpd.points_from_xy(df['x'], df['y']))
        pt.crs = {'init': "epsg:2154"}
        pts = pt.to_crs({'init': "epsg:4326"})
        df['lat'] = pts.geometry.y
        df['long'] = pts.geometry.x
    except Exception as e:
        print(f"Erreur lors de la conversion des coordonnées : {e}")

# Reformattage d'un champ
def reformat(op, field, value):
    try:
        # Si aucune reformattage n'est prévu, retourner value inchangée
        if 'reformatting' not in op or field not in op['reformatting'] or value == '':
            return value
        remap = op['reformatting'][field]  # On récupère le reformattage de ce champ
        # On filtre avec re.match et on utilise format pour print dans le nouveau format
        return remap['format'].format(*re.match(remap['match'], value).groups())
    except Exception as e:
        print(f"Erreur lors du reformattage pour {field} de {op['name']}: {e}")
        return value  # Retourne la valeur initiale en cas d'erreur

def collecte(row):
    return 'HS' if 'HS' in row else 'OK' if 'OK' in row else None

# Étape d'uniformisation
def make_op_uniform(op):
    print("Opérateur : " + op['name'])
    try:
        df = get_raw_dataframe(op)
        if df.empty:
            print(f"Le dataframe pour {op['name']} est vide, impossible de continuer.")
            return
        print("Sites HS : " + str(len(df.index)))

        # Renommage et conversion des colonnes
        df.rename(columns=op['structure'], inplace=True)
        if 'lat' not in df or 'long' not in df:
            coords_conversion(df)
        
        # Création du dataframe uniformisé
        nf = pd.DataFrame(columns=all_columns)

        for field in detail_duree_columns:
            if field in df:
                nf[field] = df[field].fillna('').astype(str).apply(lambda r: reformat(op, field, r))
            else:
                nf[field] = np.nan
        
        # Gestion des colonnes de date
        try:
            if 'debut' not in df and 'debut_data' in df and 'debut_voix' in df:
                nf['debut'] = nf.apply(lambda s: min([e for e in [s['debut_data'], s['debut_voix']] if e] or ['']), axis=1)
            if 'fin' not in df and 'fin_data' in df and 'fin_voix' in df:
                nf['fin'] = nf.apply(lambda s: max([e for e in [s['fin_data'], s['fin_voix']] if e] or ['']), axis=1)
        except Exception as e:
            print(f"Erreur lors du traitement des colonnes de date pour {op['name']}: {e}")

        # Remplissage des catégories de données manquantes
        if 'voix' not in df:
            df['voix'] = df.apply(lambda s: collecte([s['voix2g'], s['voix3g'], s['voix4g']]), axis=1)
        if 'data' not in df:
            df['data'] = df.apply(lambda s: collecte([s['data3g'], s['data4g'], s['data5g']]), axis=1)
        
        # Formatage des codes postaux et codes INSEE
        try:
            if 'code_insee' in df:
                df['code_insee'] = [re.findall('([0-9]?[0-9AB][0-9][0-9][0-9]).*', d)[0] for d in df['code_insee'].astype(str)]
                nf['code_insee'] = df['code_insee'].astype(str).str.zfill(5)
                if 'departement' not in df:
                    nf['departement'] = nf['code_insee'].str[0:2]
            if 'code_postal' in df:
                nf['code_postal'] = df['code_postal'].astype(int)
                if 'departement' not in df:
                    nf['departement'] = nf['code_postal'].astype(str).str.zfill(5).str[0:2]
        except Exception as e:
            print(f"Erreur lors du traitement des codes postaux ou INSEE pour {op['name']}: {e}")
        
        for col in equipment_columns + ['lat', 'long', 'commune']:
            nf[col] = df.get(col)
        
        nf['date'] = datename
        nf['op_code'] = op['code']
        nf['operateur'] = op['name']
        
        nf = nf.sort_values(by=['departement', 'code_insee', 'code_postal'])
        op['dataframe'] = nf
        nf.to_csv(save.op_path(op, '.csv'), sep=',', index=False)
        nf.to_json(save.op_path(op, '.json'), orient='records')
    except Exception as e:
        print(f"Échec dans make_op_uniform pour {op['name']}: {e}")

# Uniformisation des chacun des fichiers opérateurs
for op in operateurs:
    try:
        make_op_uniform(op)
    except Exception as e:
        print("Echec de l'uniformisation: " + op['name'] + f" avec erreur {e}")

# Union des dataframes uniformes générés
union_df = pd.concat([op['dataframe'] for op in operateurs if 'dataframe' in op])
# Remplace tous les NaN par None
union_df = union_df.where(pd.notnull(union_df), None)

# Sauvegarde aux formats CSV et JSON
union_df.to_csv(save.all_path('.csv'), sep=',', index=False)
union_df.to_json(save.all_path('.json'), orient='records')

# Conversion en GeoJSON
def df_to_geojson(df, properties, lat='lat', lon='long'):
    return {
        'type': 'FeatureCollection',
        'features':
            [
                {'type': 'Feature',
                 'properties': {prop: row[prop] for prop in properties},
                 'geometry': {'type': 'Point',
                              'coordinates': [row[lon], row[lat]]}}
                for _, row in df.iterrows()
            ]
    }

# Propriétés GeoJSON à intégrer
geojson_properties = ['operateur', 'departement', 'code_postal', 'code_insee', 'commune'] + equipment_columns + detail_duree_columns

# Export en GeoJSON
with open(save.all_path('.geojson'), 'w') as file:
    # Export dans le fichier au format geojson
    geojson = df_to_geojson(union_df, geojson_properties)
    file.write(json.dumps(geojson))

print("Fichiers de données générés !")
