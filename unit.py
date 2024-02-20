import uuid
from typing import Optional


class Match:
    def __init__(
        self,
        #  game_id: Optional[str] = None,
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
        _allowed_attributes = {
            '_game_id', 'event', 'site', 'date', 'round', 'white', 'black', 'result',
            'utcdate', 'utctime', 'whiteelo', 'blackelo', 'whiteratingdiff', 'blackratingdiff',
            'whitetitle', 'blacktitle', 'eco', 'opening', 'timecontrol', 'termination', 'gamemoves'
        }
        
        if name == 'game_id':
            raise AttributeError(f"game_id is immutable")
        elif name not in _allowed_attributes:
            raise AttributeError(f"Unable to change '{name}' attribute of class '{type(self).__name__}'")
        else:
            # Use the super function to bypass this __setattr__ implementation
            # and actually set the attribute
            super().__setattr__(name, value)

    @property
    def game_id(self) -> str:
        return self._game_id
    
class GameMoves:
    pass
