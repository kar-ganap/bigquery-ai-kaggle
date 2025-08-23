-- Example: find 10 nearest ads to a seed ad_id
SELECT r.brand, vs.ad_id, vs.distance, r.title, r.creative_text
FROM VECTOR_SEARCH(
       `yourproj.ads_demo.ads_emb`, 'emb',
       (SELECT emb FROM `yourproj.ads_demo.ads_emb` WHERE ad_id = 'A2'),
       top_k => 10, distance_type => 'COSINE'
     ) AS vs
JOIN `yourproj.ads_demo.ads_raw` r USING(ad_id)
ORDER BY distance;
