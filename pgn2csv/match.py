import uuid
from typing import Optional


class Match:
    __slots__ = {
        "_game_id",
        "event",
        "site",
        "date",
        "round",
        "white",
        "black",
        "result",
        "utcdate",
        "utctime",
        "whiteelo",
        "blackelo",
        "whiteratingdiff",
        "blackratingdiff",
        "whitetitle",
        "blacktitle",
        "eco",
        "opening",
        "timecontrol",
        "termination",
        "gamemoves",
    }

    def __init__(
        self,
        event: Optional[str] = None,
        site: Optional[str] = None,
        date: Optional[str] = None,
        round: Optional[str] = None,
        white: Optional[str] = None,
        black: Optional[str] = None,
        result: Optional[str] = None,
        utcdate: Optional[str] = None,
        utctime: Optional[str] = None,
        whiteelo: Optional[int] = None,
        blackelo: Optional[int] = None,
        whiteratingdiff: Optional[str] = None,
        blackratingdiff: Optional[str] = None,
        whitetitle: Optional[str] = None,
        blacktitle: Optional[str] = None,
        eco: Optional[str] = None,
        opening: Optional[str] = None,
        timecontrol: Optional[str] = None,
        termination: Optional[str] = None,
        gamemoves: Optional[str] = None,
    ):

        self._game_id = str(uuid.uuid4())
        self.event = event
        self.site = site
        self.date = date
        self.round = round
        self.white = white
        self.black = black
        self.result = result
        self.utcdate = utcdate
        self.utctime = utctime
        self.whiteelo = whiteelo
        self.blackelo = blackelo
        self.whiteratingdiff = whiteratingdiff
        self.blackratingdiff = blackratingdiff
        self.whitetitle = whitetitle
        self.blacktitle = blacktitle
        self.eco = eco
        self.opening = opening
        self.timecontrol = timecontrol
        self.termination = termination
        self.gamemoves = gamemoves

    def __setattr__(self, name, value):
        """
        Constraint a fixed schema/ list of attributes for a match.
        """

        if name == "game_id":
            raise AttributeError(f"game_id is immutable")
        elif name not in Match.__slots__:
            raise AttributeError(
                f"Unable to change '{name}' attribute of class '{type(self).__name__}'"
            )
        else:
            # Use the super function to bypass this __setattr__ implementation
            # and actually set the attribute
            super().__setattr__(name, value)

    def set_attribute(self, name, value):
        self.__setattr__(name, value)

    @property
    def game_id(self) -> str:
        return self._game_id

    def __repr__(self) -> str:
        return f"{self._game_id}"
    
    def __eq__(self, __value: object) -> bool:
        return self._game_id == __value