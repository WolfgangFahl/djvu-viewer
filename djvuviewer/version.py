"""
Created on 2024-08-15

@author: wf

"""

from dataclasses import dataclass

import djvuviewer


@dataclass
class Version(object):
    """
    Version handling for DjVuViewer
    """

    name = "djvuviewer"
    version = djvuviewer.__version__
    date = "2024-08-15"
    updated = "2026-01-10"
    description = "DjVu Viewer and package converter"

    authors = "Wolfgang Fahl"

    doc_url = "https://wiki.bitplan.com/index.php/djvu-viewer"
    chat_url = "https://github.com/WolfgangFahl/djvu-viewer/discussions"
    cm_url = "https://github.com/WolfgangFahl/djvu-viewer"

    license = f"""Copyright 2024-2026 contributors. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied."""
    longDescription = f"""{name} version {version}
{description}

  Created by {authors} on {date} last updated {updated}"""
