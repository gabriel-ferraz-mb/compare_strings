UPDATE  mq."tblInformanteCoords" AS informante
SET cd_mun = public."GEO_MUN_22"."CD_MUN" 
FROM public."GEO_MUN_22"
WHERE ST_Intersects(
    ST_SetSRID(ST_MakePoint(informante.longitude::float  , informante.latitude ::float), 4326),
    public."GEO_MUN_22".geom_4326)
;


SELECT informante."Id_informante", informante.cd_mun, db.id_cidade::text,
CASE WHEN informante."cd_mun" = db."id_cidade"::text THEN 'TRUE' ELSE 'FALSE' END AS mun_cidade_equality
FROM mq."tblInformanteCoords" AS informante
JOIN mq."mqDb" AS db ON informante."Id_informante" = db."id_informante"::text;