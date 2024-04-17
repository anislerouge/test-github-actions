import datetime

import json
import requests
import base64
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator


CHAMPS_NOMENCLATURE = {'Champ-3642770': 'Vos coordonnées',
 'Champ-3642774': 'Adresse électronique',
 'Champ-3642775': 'Numéro de téléphone',
 'Champ-3642777': 'Vous formulez cette déclaration en tant que :',
 'Champ-3642778': 'Raison sociale de votre structure',
 'Champ-2378853': "Point de prélèvement d'eau",
 'Champ-3888472': 'Type de prélèvement',
 'Champ-3915146': "Numéro de votre arrêté d'AOT",
 'Champ-3888489': 'Prélèvement par camion citerne',
 'Champ-3988566': 'Remontée des volumes prélevés',
 'Champ-3902209': 'En quelle année les prélèvements que vous allez déclarer ont-ils été réalisés ?',
 'Champ-3988469': 'Comment souhaitez-vous transmettre vos données ?',
 'Champ-3888513': 'Dans cette partie, vous allez pouvoir renseigner les volumes pompés par jour sur chaque point de prélèvement',
 'Champ-3888490': 'Volumes pompés',
 'Champ-3988564': 'Copie du registre papier',
 'Champ-3915100': 'Extrait de registre',
 'Champ-2379084': 'Pour finir',
 'Champ-3645094': 'Commentaire',
 'Champ-2379086': "En cochant la présente case, je déclare que les informations que j'ai complété dans le questionnaire sont exactes"}

CHAMPS_COLUMNS_NAMES = {'Champ-3642770': 'coordonnees',
 'Champ-3642774': 'adresseEmail',
 'Champ-3642775': 'numeroTelephone',
 'Champ-3642777': 'position',
 'Champ-3642778': 'raisonSociale',
 'Champ-2378853': "pointPrelevementEau",
 'Champ-3888472': 'typePrelevement',
 'Champ-3915146': "numeroArreteAOT",
 'Champ-3888489': 'prelevementCiterne',
 'Champ-3988566': 'remonteeVolumePreleve',
 'Champ-3902209': 'anneePrelevement',
 'Champ-3988469': 'moyenTransimissionDonnee',
 'Champ-3888513': 'volumesPompesJour',
 'Champ-3888490': 'volumesPompes',
 'Champ-3988564': 'copieRegistrePapier',
 'Champ-3915100': 'extraitRegistre',
 'Champ-2379084': 'PourFinir',
 'Champ-3645094': 'commentaire',
 'Champ-2379086': "validationInformation"}

DS_URL="https://www.demarches-simplifiees.fr/api/v2/graphql"

DS_TOKEN = os.environ["DS_TOKEN"]


def decode64(code: str):
    encoded_string = code
    decoded_bytes = base64.b64decode(encoded_string)
    decoded_string = decoded_bytes.decode('utf-8')
    return decoded_string


def getDemarcheFromDS():
    response = requests.post(
      url=DS_URL,
      headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DS_TOKEN}"
      },
      json={
        "query":" query getDemarche($demarcheNumber: Int! $state: DossierState $order: Order $first: Int $last: Int $before: String $after: String $archived: Boolean $revision: ID $createdSince: ISO8601DateTime $updatedSince: ISO8601DateTime $pendingDeletedFirst: Int $pendingDeletedLast: Int $pendingDeletedBefore: String $pendingDeletedAfter: String  $pendingDeletedSince: ISO8601DateTime  $deletedFirst: Int  $deletedLast: Int  $deletedBefore: String  $deletedAfter: String  $deletedSince: ISO8601DateTime  $includeGroupeInstructeurs: Boolean = false  $includeDossiers: Boolean = false  $includePendingDeletedDossiers: Boolean = false  $includeDeletedDossiers: Boolean = false  $includeRevision: Boolean = false  $includeService: Boolean = false  $includeChamps: Boolean = true  $includeAnotations: Boolean = true  $includeTraitements: Boolean = true  $includeInstructeurs: Boolean = true  $includeAvis: Boolean = false  $includeMessages: Boolean = false  $includeCorrections: Boolean = false  $includeGeometry: Boolean = false ) {  demarche(number: $demarcheNumber) {   id   number   title   state   declarative   dateCreation   dateFermeture   chorusConfiguration {    centreDeCout    domaineFonctionnel    referentielDeProgrammation   }   activeRevision @include(if: $includeRevision) {    ...RevisionFragment   }   groupeInstructeurs @include(if: $includeGroupeInstructeurs) {    ...GroupeInstructeurFragment   }   service @include(if: $includeService) {    ...ServiceFragment   }   dossiers(    state: $state    order: $order    first: $first    last: $last    before: $before    after: $after    archived: $archived    createdSince: $createdSince    updatedSince: $updatedSince    revision: $revision   ) @include(if: $includeDossiers) {    pageInfo {     ...PageInfoFragment    }    nodes {     ...DossierFragment    }   }   pendingDeletedDossiers(    first: $pendingDeletedFirst    last: $pendingDeletedLast    before: $pendingDeletedBefore    after: $pendingDeletedAfter    deletedSince: $pendingDeletedSince   ) @include(if: $includePendingDeletedDossiers) {    pageInfo {     ...PageInfoFragment    }    nodes {     ...DeletedDossierFragment    }   }   deletedDossiers(    first: $deletedFirst    last: $deletedLast    before: $deletedBefore    after: $deletedAfter    deletedSince: $deletedSince   ) @include(if: $includeDeletedDossiers) {    pageInfo {     ...PageInfoFragment    }    nodes {     ...DeletedDossierFragment    }   }  } } query getGroupeInstructeur(  $groupeInstructeurNumber: Int!  $state: DossierState  $order: Order  $first: Int  $last: Int  $before: String  $after: String  $archived: Boolean  $revision: ID  $createdSince: ISO8601DateTime  $updatedSince: ISO8601DateTime  $pendingDeletedOrder: Order  $pendingDeletedFirst: Int  $pendingDeletedLast: Int  $pendingDeletedBefore: String  $pendingDeletedAfter: String  $pendingDeletedSince: ISO8601DateTime  $deletedOrder: Order  $deletedFirst: Int  $deletedLast: Int  $deletedBefore: String  $deletedAfter: String  $deletedSince: ISO8601DateTime  $includeDossiers: Boolean = false  $includePendingDeletedDossiers: Boolean = false  $includeDeletedDossiers: Boolean = false  $includeChamps: Boolean = true  $includeAnotations: Boolean = true  $includeTraitements: Boolean = true  $includeInstructeurs: Boolean = true  $includeAvis: Boolean = false  $includeMessages: Boolean = false  $includeCorrections: Boolean = false  $includeGeometry: Boolean = false ) {  groupeInstructeur(number: $groupeInstructeurNumber) {   id   number   label   instructeurs @include(if: $includeInstructeurs) {    id    email   }   dossiers(    state: $state    order: $order    first: $first    last: $last    before: $before    after: $after    archived: $archived    createdSince: $createdSince    updatedSince: $updatedSince    revision: $revision   ) @include(if: $includeDossiers) {    pageInfo {     ...PageInfoFragment    }    nodes {     ...DossierFragment    }   }   pendingDeletedDossiers(    order: $pendingDeletedOrder    first: $pendingDeletedFirst    last: $pendingDeletedLast    before: $pendingDeletedBefore    after: $pendingDeletedAfter    deletedSince: $pendingDeletedSince   ) @include(if: $includePendingDeletedDossiers) {    pageInfo {     ...PageInfoFragment    }    nodes {     ...DeletedDossierFragment    }   }   deletedDossiers(    order: $deletedOrder    first: $deletedFirst    last: $deletedLast    before: $deletedBefore    after: $deletedAfter    deletedSince: $deletedSince   ) @include(if: $includeDeletedDossiers) {    pageInfo {     ...PageInfoFragment    }    nodes {     ...DeletedDossierFragment    }   }  } } query getDossier(  $dossierNumber: Int!  $includeRevision: Boolean = false  $includeService: Boolean = false  $includeChamps: Boolean = true  $includeAnotations: Boolean = true  $includeTraitements: Boolean = true  $includeInstructeurs: Boolean = true  $includeAvis: Boolean = false  $includeMessages: Boolean = false  $includeCorrections: Boolean = false  $includeGeometry: Boolean = false ) {  dossier(number: $dossierNumber) {   ...DossierFragment   demarche {    ...DemarcheDescriptorFragment   }  } } query getDemarcheDescriptor(  $demarche: FindDemarcheInput!  $includeRevision: Boolean = false  $includeService: Boolean = false ) {  demarcheDescriptor(demarche: $demarche) {   ...DemarcheDescriptorFragment  } } fragment ServiceFragment on Service {  nom  siret  organisme  typeOrganisme } fragment GroupeInstructeurFragment on GroupeInstructeur {  id  number  label  instructeurs @include(if: $includeInstructeurs) {   id   email  } } fragment DossierFragment on Dossier {  __typename  id  number  archived  prefilled  state  dateDerniereModification  dateDepot  datePassageEnConstruction  datePassageEnInstruction  dateTraitement  dateExpiration  dateSuppressionParUsager  dateDerniereCorrectionEnAttente @include(if: $includeCorrections)  motivation  motivationAttachment {   ...FileFragment  }  attestation {   ...FileFragment  }  pdf {   ...FileFragment  }  usager {   email  }  prenomMandataire  nomMandataire  deposeParUnTiers  connectionUsager  groupeInstructeur {   ...GroupeInstructeurFragment  }  demandeur {   __typename   ...PersonnePhysiqueFragment   ...PersonneMoraleFragment   ...PersonneMoraleIncompleteFragment  }  demarche {   revision {    id   }  }  instructeurs @include(if: $includeInstructeurs) {   id   email  }  traitements @include(if: $includeTraitements) {   state   emailAgentTraitant   dateTraitement   motivation  }  champs @include(if: $includeChamps) {   ...ChampFragment   ...RootChampFragment  }  annotations @include(if: $includeAnotations) {   ...ChampFragment   ...RootChampFragment  }  avis @include(if: $includeAvis) {   ...AvisFragment  }  messages @include(if: $includeMessages) {   ...MessageFragment  } } fragment DemarcheDescriptorFragment on DemarcheDescriptor {  id  number  title  description  state  declarative  dateCreation  datePublication  dateDerniereModification  dateDepublication  dateFermeture  notice { url }  deliberation { url }  demarcheURL  cadreJuridiqueURL  service @include(if: $includeService) {   ...ServiceFragment  }  revision @include(if: $includeRevision) {   ...RevisionFragment  } } fragment DeletedDossierFragment on DeletedDossier {  id  number  dateSupression  state  reason } fragment RevisionFragment on Revision {  id  datePublication  champDescriptors {   ...ChampDescriptorFragment   ... on RepetitionChampDescriptor {    champDescriptors {     ...ChampDescriptorFragment    }   }  }  annotationDescriptors {   ...ChampDescriptorFragment   ... on RepetitionChampDescriptor {    champDescriptors {     ...ChampDescriptorFragment    }   }  } } fragment ChampDescriptorFragment on ChampDescriptor {  __typename  id  label  description  required  ... on DropDownListChampDescriptor {   options   otherOption  }  ... on MultipleDropDownListChampDescriptor {   options  }  ... on LinkedDropDownListChampDescriptor {   options  }  ... on PieceJustificativeChampDescriptor {   fileTemplate {    ...FileFragment   }  }  ... on ExplicationChampDescriptor {   collapsibleExplanationEnabled   collapsibleExplanationText  } } fragment AvisFragment on Avis {  id  question  reponse  dateQuestion  dateReponse  claimant {   email  }  expert {   email  }  attachments {   ...FileFragment  } } fragment MessageFragment on Message {  id  email  body  createdAt  attachments {   ...FileFragment  }  correction @include(if: $includeCorrections) {   reason   dateResolution  } } fragment GeoAreaFragment on GeoArea {  id  source  description  geometry @include(if: $includeGeometry) {   type   coordinates  }  ... on ParcelleCadastrale {   commune   numero   section   prefixe   surface  } } fragment RootChampFragment on Champ {  ... on RepetitionChamp {   rows {    champs {     ...ChampFragment    }   }  }  ... on CarteChamp {   geoAreas {    ...GeoAreaFragment   }  }  ... on DossierLinkChamp {   dossier {    id    number    state   }  } } fragment ChampFragment on Champ {  id  champDescriptorId  __typename  label  stringValue  updatedAt  prefilled  ... on DateChamp {   date  }  ... on DatetimeChamp {   datetime  }  ... on CheckboxChamp {   checked: value  }  ... on DecimalNumberChamp {   decimalNumber: value  }  ... on IntegerNumberChamp {   integerNumber: value  }  ... on CiviliteChamp {   civilite: value  }  ... on LinkedDropDownListChamp {   primaryValue   secondaryValue  }  ... on MultipleDropDownListChamp {   values  }  ... on PieceJustificativeChamp {   files {    ...FileFragment   }  }  ... on AddressChamp {   address {    ...AddressFragment   }   commune {    ...CommuneFragment   }   departement {    ...DepartementFragment   }  }  ... on EpciChamp {   epci {    ...EpciFragment   }   departement {    ...DepartementFragment   }  }  ... on CommuneChamp {   commune {    ...CommuneFragment   }   departement {    ...DepartementFragment   }  }  ... on DepartementChamp {   departement {    ...DepartementFragment   }  }  ... on RegionChamp {   region {    ...RegionFragment   }  }  ... on PaysChamp {   pays {    ...PaysFragment   }  }  ... on SiretChamp {   etablissement {    ...PersonneMoraleFragment   }  }  ... on RNFChamp {   rnf {    ...RNFFragment   }   commune {    ...CommuneFragment   }   departement {    ...DepartementFragment   }  }  ... on EngagementJuridiqueChamp {   engagementJuridique {    ...EngagementJuridiqueFragment   }  } } fragment PersonneMoraleFragment on PersonneMorale {  siret  siegeSocial  naf  libelleNaf  address {   ...AddressFragment  }  entreprise {   siren   capitalSocial   numeroTvaIntracommunautaire   formeJuridique   formeJuridiqueCode   nomCommercial   raisonSociale   siretSiegeSocial   codeEffectifEntreprise   dateCreation   nom   prenom   attestationFiscaleAttachment {    ...FileFragment   }   attestationSocialeAttachment {    ...FileFragment   }  }  association {   rna   titre   objet   dateCreation   dateDeclaration   datePublication  } } fragment PersonneMoraleIncompleteFragment on PersonneMoraleIncomplete {  siret } fragment PersonnePhysiqueFragment on PersonnePhysique {  civilite  nom  prenom  email } fragment FileFragment on File {  __typename  filename  contentType  checksum  byteSize: byteSizeBigInt  url  createdAt } fragment AddressFragment on Address {  label  type  streetAddress  streetNumber  streetName  postalCode  cityName  cityCode  departmentName  departmentCode  regionName  regionCode } fragment PaysFragment on Pays {  name  code } fragment RegionFragment on Region {  name  code } fragment DepartementFragment on Departement {  name  code } fragment EpciFragment on Epci {  name  code } fragment CommuneFragment on Commune {  name  code  postalCode } fragment RNFFragment on RNF {  id  title  address {   ...AddressFragment  } } fragment EngagementJuridiqueFragment on EngagementJuridique {  montantEngage  montantPaye } fragment PageInfoFragment on PageInfo {  hasPreviousPage  hasNextPage  startCursor  endCursor }",
        "variables": {
          "demarcheNumber":80149,
          "includeDossiers":True
        },
        "operationName":"getDemarche"
      })
    data = json.loads(response.content)
    return data

def extractDataFromDirectory(directory):
    result = {}
    result["id"] = directory["id"]
    result["number"] = directory["number"]
    result["state"] = directory["state"]
    
    result["demandeurType"] = directory["demandeur"]["__typename"]
    result["demandeurCivilite"] = directory["demandeur"]["civilite"]
    result["demandeurNom"] = directory["demandeur"]["nom"]
    result["demandeurPrenom"] = directory["demandeur"]["prenom"]
    result["demandeurEmail"] = directory["demandeur"]["email"]
    

    for i in data["data"]["demarche"]["dossiers"]["nodes"][0]["champs"]:
        result[CHAMPS_COLUMNS_NAMES[decode64(i["id"])]] = i["stringValue"]
    return result




with DAG(
    dag_id="dag_collect_data_from_ds",
    schedule_interval = datetime.timedelta(minutes=5),
        start_date=datetime.datetime(2021, 1, 1),
):
    data = getDemarcheFromDS()
    dossiers = []
    for i in range(len(data["data"]["demarche"]["dossiers"]["nodes"])):
        dossiers.append(extractDataFromDirectory(data["data"]["demarche"]["dossiers"]["nodes"][i]))
