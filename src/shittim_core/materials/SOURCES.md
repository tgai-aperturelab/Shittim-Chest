# Materials source notes

`names.py::sanitize_name` adapts AstrBot's `_sanitize_file_component_name` from
`astrbot/core/message/components.py:757-767` at commit
`e5e21b71aec5642f110eb115eb8e9a0d8028b7e5` (AGPL-3.0-or-later).

Source: https://github.com/AstrBotDevs/AstrBot/blob/e5e21b71aec5642f110eb115eb8e9a0d8028b7e5/astrbot/core/message/components.py#L757

Pinned checkout file SHA-256:
`5cee4bbc740e875b1322042cddf3b19a248f6a554fc351f8160a258e18ac5029`.

Retained: normalize Windows separators, basename extraction, remove NUL, replace
invalid Windows filename characters, safe fallback for empty/dot names.
Added: strip control characters/trailing dots and cap display-name length. Storage
paths prepend a random identifier, independent of the original path.

The material store is project-owned code using the existing StateStore; it does
not create another metadata database. It stores bounded bytes, immutable versions,
source references and checksums, and refuses cross-context/audience/capability reads
and linked or out-of-root metadata paths. `downloader.py` adds explicit HTTP retrieval
with a caller-supplied client and exact host allowlist, no URL credentials, no redirects,
uncompressed transfer, byte caps and a total timeout. It is not DNS-rebinding protection.
It does not implement document conversion or a full material-result wakeup pipeline.

The root must be local and controlled by the application. The implementation is not
a filesystem sandbox against a hostile OS administrator or arbitrary external
directory replacement. Crashes between file creation and metadata commit can leave
an unregistered file; files are not automatically swept or overwritten.
