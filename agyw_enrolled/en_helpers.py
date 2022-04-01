from re import sub

def name_handler(s):
    s = sub(r"[^\w\s]", '', s)
    # Replace all runs of whitespace with a single dash
    s = sub(r"\s+", '_', s)
    return s


def commune_to_department(d):
    if (
      d == 'petion_ville' or  
      d == 'kenscoff' or  
      d == 'carrefour' or  
      d == 'portauprince' or 
      d == 'delmas' or 
      d == 'gressier' or 
      d == 'tabarre' 
    ):
        return "Ouest"
    elif (
        d == 'dessalines' or  
      d == 'verrettes' or  
      d == 'saintmarc' or  
      d == 'la_chapelle' or 
      d == 'liancourt' or 
      d == 'petite_rivire_de_lartibonite' or 
      d == 'grandesaline' or 
      d == 'desdunes' or 
      d == 'montrouis'
    ):
        return "Artibonite"
    elif(
        d == 'caphatien' or  
      d == 'limonade' or  
      d == 'quartiermorin' or  
      d == 'la_chapelle' or 
      d == 'plainedunord' or 
      d == 'granderiviredunord' or 
      d == 'troudunord' or 
      d == 'milot'
    ):
        return "Nord"
    elif(
        d=='chantal'
    ):
        return 'Sud'
    else:
        return "Id_commune_first"