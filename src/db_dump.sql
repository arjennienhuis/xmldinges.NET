CREATE TYPE pand_status AS ENUM (
    'Pand gesloopt',
    'Bouwvergunning verleend',
    'Bouw gestart',
    'Pand in gebruik',
    'Pand buiten gebruik',
    'Sloopvergunning verleend',
    'Niet gerealiseerd pand',
    'Pand in gebruik (niet ingemeten)'
);

CREATE TYPE status_naamgeving AS ENUM (
    'Naamgeving uitgegeven',
    'Naamgeving ingetrokken'
);

CREATE TYPE type_adresseerbaar_object AS ENUM (
    'Verblijfsobject',
    'Ligplaats',
    'Standplaats'
);

CREATE TYPE verblijfsobjectgebruiksdoel AS ENUM (
    'kantoorfunctie',
    'woonfunctie',
    'logiesfunctie',
    'winkelfunctie',
    'overige gebruiksfunctie',
    'gezondheidszorgfunctie',
    'sportfunctie',
    'onderwijsfunctie',
    'industriefunctie',
    'bijeenkomstfunctie',
    'celfunctie'
);

CREATE TYPE verblijfsobjectstatus AS ENUM (
    'Verblijfsobject in gebruik',
    'Verblijfsobject gevormd',
    'Verblijfsobject in gebruik (niet ingemeten)',
    'Verblijfsobject ingetrokken',
    'Niet gerealiseerd verblijfsobject',
    'Verblijfsobject buiten gebruik'
);

CREATE TYPE status_woonplaats AS ENUM (
    'Woonplaats aangewezen',
    'Woonplaats ingetrokken'
);

CREATE DOMAIN bag_id AS varchar COLLATE "C";
CREATE DOMAIN bag_ids AS varchar[] COLLATE "C";

CREATE TYPE type_openbare_ruimte AS ENUM (
    'Weg',
    'Water',
    'Spoorbaan',
    'Terrein',
    'Kunstwerk',
    'Landschappelijk gebied',
    'Administratief gebied'
);

CREATE TYPE status_openbare_ruimte AS ENUM (
    'Naamgeving uitgegeven',
    'Naamgeving ingetrokken'
);

CREATE TYPE plaatsstatus AS ENUM (
    'Plaats aangewezen',
    'Plaats ingetrokken'
);


CREATE TABLE openbare_ruimte (
    identificatie bag_id NOT NULL,
    inactief boolean NOT NULL,
    correctienummer integer NOT NULL,
    naam varchar NOT NULL,
    officieel boolean NOT NULL,
    begindatumtijdvakgeldigheid timestamp with time zone NOT NULL,
    einddatumtijdvakgeldigheid timestamp with time zone,
    inonderzoek boolean NOT NULL,
    woonplaats_id bag_id NOT NULL,
    type type_openbare_ruimte NOT NULL,
    status status_openbare_ruimte NOT NULL
);

CREATE TABLE nummeraanduiding (
    identificatie bag_id NOT NULL,
    inactief boolean NOT NULL,
    correctienummer integer NOT NULL,
    officieel boolean NOT NULL,
    huisnummer integer NOT NULL,
    huisletter character varying,
    toevoeging character varying,
    postcode character varying,
    woonplaats_id bag_id,
    begindatumtijdvakgeldigheid timestamp with time zone NOT NULL,
    einddatumtijdvakgeldigheid timestamp with time zone,
    inonderzoek boolean NOT NULL,
    openbare_ruimte_id bag_id NOT NULL,
    type type_adresseerbaar_object NOT NULL,
    status status_naamgeving NOT NULL
);

CREATE TABLE pand (
    identificatie bag_id NOT NULL,
    inactief boolean NOT NULL,
    correctienummer integer NOT NULL,
    officieel boolean NOT NULL,
    geometrie geometry NOT NULL,
    bouwjaar integer NOT NULL,
    status pand_status NOT NULL,
    begindatumtijdvakgeldigheid timestamp with time zone NOT NULL,
    einddatumtijdvakgeldigheid timestamp with time zone,
    inonderzoek boolean NOT NULL
);

CREATE TABLE verblijfsobject (
    identificatie bag_id NOT NULL,
    inactief boolean NOT NULL,
    correctienummer integer NOT NULL,
    begindatumtijdvakgeldigheid timestamp with time zone NOT NULL,
    einddatumtijdvakgeldigheid timestamp with time zone,
    inonderzoek boolean NOT NULL,
    verblijfsobjectstatus verblijfsobjectstatus NOT NULL,
    verblijfsobjectgeometrie geometry NOT NULL,
    officieel boolean NOT NULL,
    nevenadres_ids bag_ids NOT NULL,
    hoofdadres_id bag_id NOT NULL,
    oppervlakte numeric NOT NULL,
    gebruiksdoelen verblijfsobjectgebruiksdoel[] NOT NULL,
    pand_ids bag_ids NOT NULL
);

CREATE TABLE woonplaats (
    identificatie bag_id NOT NULL,
    inactief boolean NOT NULL,
    correctienummer integer NOT NULL,
    woonplaatsnaam character varying NOT NULL,
    begindatumtijdvakgeldigheid timestamp with time zone NOT NULL,
    einddatumtijdvakgeldigheid timestamp with time zone,
    inonderzoek boolean NOT NULL,
    woonplaatsstatus status_woonplaats NOT NULL,
    woonplaatsgeometrie geometry NOT NULL,
    officieel boolean NOT NULL
);

CREATE TABLE standplaats (
    identificatie bag_id NOT NULL,
    inactief boolean NOT NULL,
    correctienummer integer NOT NULL,
    officieel boolean NOT NULL,
    standplaatsstatus plaatsstatus NOT NULL,
    hoofdadres_id bag_id NOT NULL,
    nevenadres_ids bag_ids NOT NULL,
    standplaatsgeometrie geometry NOT NULL,
    inonderzoek boolean NOT NULL,
    begindatumtijdvakgeldigheid timestamp with time zone NOT NULL,
    einddatumtijdvakgeldigheid timestamp with time zone
);

CREATE TABLE ligplaats (
    identificatie bag_id NOT NULL,
    inactief boolean NOT NULL,
    correctienummer integer NOT NULL,
    officieel boolean NOT NULL,
    ligplaatsstatus plaatsstatus NOT NULL,
    hoofdadres_id bag_id NOT NULL,
    nevenadres_ids bag_ids NOT NULL,
    ligplaatsgeometrie geometry NOT NULL,
    inonderzoek boolean NOT NULL,
    begindatumtijdvakgeldigheid timestamp with time zone NOT NULL,
    einddatumtijdvakgeldigheid timestamp with time zone
);

CREATE TABLE perceel (
    identificatie varchar NOT NULL,
    geometrie geometry NOT NULL
);


CREATE INDEX "idx_pand_geometrie" ON pand USING gist (geometrie);
CREATE INDEX "idx_verblijfsobject_geometrie" ON verblijfsobject USING gist (verblijfsobjectgeometrie);
CREATE INDEX "idx_woonplaats_geometrie" ON woonplaats USING gist (woonplaatsgeometrie);
CREATE INDEX "idx_standplaats_geometrie" ON standplaats USING gist (standplaatsgeometrie);
CREATE INDEX "idx_ligplaats_geometrie" ON ligplaats USING gist (ligplaatsgeometrie);
CREATE INDEX "idx_perceel_geometrie" ON perceel USING gist (geometrie);



-- CREATE UNIQUE INDEX woonplaats_identificatie_idx ON woonplaats USING btree (identificatie) WHERE ((einddatumtijdvakgeldigheid IS NULL) AND (NOT inactief));

