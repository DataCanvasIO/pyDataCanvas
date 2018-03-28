import json
import pickle
import pandas as pd

def sklearn_model_predict(model, df_x, result_column):
    predict_y = model.predict(df_x)
    return pd.DataFrame({result_column:predict_y})

def main(params, inputs, outputs):
    model = inputs.model
    X = inputs.X
    Y = outputs.Y
    result_column = params.result_col_name
    
    with open(model, "rb") as f:
        model_json = f.read()
    model_type = json.loads(model_json)["model_type"]
    model_path = json.loads(model_json)["model_path"]

    if model_type == "sklearn-0.19.0":
        model = pickle.load(open(model_path, "rb"))
        print("[load model]\n%s\n[Done]"%model)
        
        df_x = pd.read_pickle(X)
        print("[load X]\n%s\n[Done]"%df_x.head())
        
        df_y = sklearn_model_predict(model, df_x, result_column)
        print("[predict]\n%s\n[Done]"%df_y.head())
        #df_y.to_pickle(Y)
        pickle.dump(df_y, open(Y,"wb"))