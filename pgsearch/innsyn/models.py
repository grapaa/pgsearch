from pydantic import BaseModel


class Vedlegg(BaseModel):
    nummer: int = 0
    navn: str = ""
    dokument_id: str = ""
    url: str = ""


class MerInfo(BaseModel):
    status: str = ""
    enhet: str = ""
    brevdato: str = ""
    type: str = ""
    kommentar: str = ""
    malebrevsnummer: str = ""
    avsender_mottaker: str = ""
    saksbehandler: str = ""
    gnr_bnr: str = ""
    sakstype: str = ""


class ByggesakDokument(BaseModel):
    saksnr: str = ""
    dato: str = ""
    gnr_bnr: str = ""
    sakstype: str = ""
    beskrivelse: str = ""
    avsender_mottaker: str = ""
    saksbehandler: str = ""
    dokument_status: str = ""
    dokument_url: str = ""
    er_tilgjengelig: bool = False
    vedlegg: list[Vedlegg] = []
    mer_info: MerInfo = MerInfo()
