# -*- coding: utf-8 -*-

import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_validate
from sklearn.preprocessing import LabelEncoder


def main(params, inputs, outputs):
    model_name = params.model_name
    cv = params.CV
    # metric = params.metric
    metric = eval("[%s]" % params.metrics)

    df_x = inputs.X.get_raw()
    df_y = inputs.Y.get_raw()
    le = LabelEncoder()
    enc_df_y = le.fit_transform(df_y)

    lr = LogisticRegression()
    lr.fit(df_x, df_y)
    print("\n[coef_]\n" % lr.coef_)
    outputs.model.put_raw(lr)

    # Cross Validate
    scores = cross_validate(lr, df_x, enc_df_y, cv=cv, scoring=metric, return_train_score=False)
    print("\n[cv scores]\n%s" % scores)
    k = scores.keys()
    v = [list(v) for v in scores.values()]
    scores = dict(zip(k, v))

    lr_meta = {
        "model_name": model_name,
        "model_type": "sklearn-%s" % sklearn.__version__,
        "pkg_depend": "from sklearn.linear_model import LogisticRegression",
        "metrics": scores
    }
    print("\n[lr meta]\n%s" % lr_meta)
    outputs.model_meta.put_raw(lr_meta)
