"use strict";

(function() {
    var CACHE_SERVICE_PORT = "8765";
    var CACHE_SERVICE_BASE = (window.MODERN_IMAGE_CACHE_API_BASE || "").replace(/\/+$/, "");
    var AUTO_CACHE = {};
    var PENDING = {};
    var INDEX_STATE = "idle";
    var INDEX_PROMISE = null;
    var INDEX_URL = "assets/aircraft/types/index.json";
    var TYPE_ASSETS = {
        B739: {
            asset: "assets/aircraft/types/B739.jpg",
            title: "Boeing 737-900 reference",
            caption: "United Airlines Boeing 737-900. Photo by Quinti9, CC BY-SA 4.0."
        },
        B772: {
            asset: "assets/aircraft/types/B772.jpg",
            title: "Boeing 777-200 reference",
            caption: "Delta Boeing 777-200. Public domain photo by Skyguy414."
        },
        B738: {
            asset: "assets/aircraft/types/B738.jpg",
            title: "Boeing 737-800 reference",
            caption: "Delta Air Lines Boeing 737-800. Photo by Pylon757, CC BY 2.0."
        },
        B38M: {
            asset: "assets/aircraft/types/B38M.jpg",
            title: "Boeing 737 MAX 8 reference",
            caption: "WestJet Boeing 737 MAX 8. Photo by Acefitt, CC BY-SA."
        },
        A320: {
            asset: "assets/aircraft/types/A320.jpg",
            title: "Airbus A320 reference",
            caption: "Condor Airbus A320 at Frankfurt Airport. Public domain photo by Dmitry A. Mottl."
        },
        A321: {
            asset: "assets/aircraft/types/A321.jpg",
            title: "Airbus A321 reference",
            caption: "Thai VietJet Air Airbus A321. Photo by AKS.9955, CC BY-SA 4.0."
        },
        E175: {
            asset: "assets/aircraft/types/E175.jpg",
            title: "Embraer E175 reference",
            caption: "KLM Cityhopper Embraer E175. Photo by AndrewDawes, CC BY-SA 4.0."
        },
        CRJ9: {
            asset: "assets/aircraft/types/CRJ9.jpg",
            title: "Canadair Regional Jet CRJ-900 reference",
            caption: "Canadair Regional Jet CRJ-900. Source photo by Icelechi, CC BY-SA 4.0."
        },
        C208: {
            asset: "assets/aircraft/types/C208.jpg",
            title: "Cessna 208 Caravan reference",
            caption: "Cessna 208 Caravan Amphibian. Photo by Rudolf Schider, public domain."
        },
        PC12: {
            asset: "assets/aircraft/types/PC12.jpg",
            title: "Pilatus PC-12 reference",
            caption: "Pilatus PC-12. Photo by Bzuk, public domain."
        }
    };

    var TYPE_ALIASES = {
        B37M: "B738",
        A21N: "A321",
        E75L: "E175",
        E75S: "E175",
        CL65: "CRJ9"
    };

    function getSelectedPlane() {
        if (typeof SelectedPlane === "undefined" || !SelectedPlane || SelectedPlane === "ICAO") {
            return null;
        }
        if (typeof Planes === "undefined") {
            return null;
        }
        return Planes[SelectedPlane] || null;
    }

    function setText(id, value) {
        var node = document.getElementById(id);
        if (node) {
            node.textContent = value;
        }
    }

    function resolveAssetForPlane(plane) {
        if (!plane || !plane.icaotype) {
            return null;
        }

        var type = plane.icaotype.toUpperCase().trim();
        var normalized = TYPE_ALIASES[type] || type;
        return {
            type: normalized,
            asset: TYPE_ASSETS[normalized] || AUTO_CACHE[normalized] || null
        };
    }

    function assetFriendlyName(asset) {
        if (!asset || !asset.title) {
            return null;
        }
        return String(asset.title).replace(/\s+reference$/i, "").trim() || null;
    }

    function formatTypeDisplayForPlane(plane) {
        if (!plane || !plane.icaotype) {
            return "n/a";
        }

        var resolved = resolveAssetForPlane(plane);
        var code = (resolved && resolved.type) ? resolved.type : String(plane.icaotype).toUpperCase().trim();
        var friendly = resolved ? assetFriendlyName(resolved.asset) : null;
        if (friendly) {
            return friendly + " (" + code + ")";
        }
        return code;
    }

    function updateTypeDisplays() {
        var plane = getSelectedPlane();
        setText("selected_icaotype", formatTypeDisplayForPlane(plane));

        if (typeof HighlightedPlane !== "undefined" && HighlightedPlane && typeof Planes !== "undefined" && Planes[HighlightedPlane]) {
            setText("higlighted_icaotype", formatTypeDisplayForPlane(Planes[HighlightedPlane]));
        }

        if (typeof PlanesOrdered !== "undefined" && PlanesOrdered && typeof PlaneRowTemplate !== "undefined" && PlaneRowTemplate && PlaneRowTemplate.cells.length > 4) {
            for (var i = 0; i < PlanesOrdered.length; i++) {
                var tableplane = PlanesOrdered[i];
                if (tableplane && tableplane.tr && tableplane.tr.cells && tableplane.tr.cells.length > 4) {
                    tableplane.tr.cells[4].textContent = tableplane.icaotype ? formatTypeDisplayForPlane(tableplane) : "";
                }
            }
        }
    }

    function cacheServiceUrls(type) {
        var urls = [];
        var protocol = window.location.protocol || "http:";
        var hostname = window.location.hostname || "piaware.local";
        urls.push(protocol + "//" + hostname + ":" + CACHE_SERVICE_PORT + "/resolve?type=" + encodeURIComponent(type));
        if (CACHE_SERVICE_BASE) {
            urls.push(CACHE_SERVICE_BASE + "/resolve?type=" + encodeURIComponent(type));
        }
        return urls;
    }

    function fetchJsonWithFallback(urls) {
        var remaining = (urls || []).slice();

        function attempt() {
            if (!remaining.length) {
                return Promise.reject(new Error("all fetch attempts failed"));
            }

            return fetch(remaining.shift())
                .then(function(response) {
                    if (!response.ok) {
                        throw new Error("request failed");
                    }
                    return response.json();
                })
                .catch(function() {
                    return attempt();
                });
        }

        return attempt();
    }

    function loadAutoCacheIndex() {
        if (INDEX_PROMISE) {
            return INDEX_PROMISE;
        }

        INDEX_STATE = "loading";
        INDEX_PROMISE = fetch(INDEX_URL, { cache: "no-cache" })
            .then(function(response) {
                if (!response.ok) {
                    throw new Error("index fetch failed");
                }
                return response.json();
            })
            .then(function(payload) {
                AUTO_CACHE = payload || {};
                INDEX_STATE = "ready";
                return AUTO_CACHE;
            })
            .catch(function() {
                INDEX_STATE = "error";
                return {};
            });

        return INDEX_PROMISE;
    }

    function requestAutoCache(type) {
        if (!type || PENDING[type]) {
            return;
        }

        PENDING[type] = true;
        fetchJsonWithFallback(cacheServiceUrls(type))
            .then(function(payload) {
                if (payload && payload.status === "ready" && payload.asset) {
                    AUTO_CACHE[type] = {
                        asset: payload.asset,
                        title: payload.title,
                        caption: payload.caption
                    };
                    updateTypeDisplays();
                    updateThumbnail();
                }
            })
            .catch(function() {
                return null;
            })
            .finally(function() {
                PENDING[type] = false;
            });
    }

    function updateThumbnail() {
        var container = document.getElementById("selected_aircraft_thumbnail");
        var thumbImage = document.getElementById("selected_aircraft_thumbnail_image");
        var modalImage = document.getElementById("aircraft_thumbnail_modal_image");
        if (!container || !thumbImage || !modalImage) {
            return;
        }

        var plane = getSelectedPlane();
        var resolved = resolveAssetForPlane(plane);
        var asset = resolved ? resolved.asset : null;
        if (!plane || !asset) {
            container.classList.add("hidden");
            if (resolved && resolved.type && INDEX_STATE !== "loading") {
                requestAutoCache(resolved.type);
            }
            return;
        }

        thumbImage.src = asset.asset;
        thumbImage.alt = asset.title;
        modalImage.src = asset.asset;
        modalImage.alt = asset.title;
        setText("selected_aircraft_thumbnail_title", asset.title);
        setText("selected_aircraft_thumbnail_caption", asset.caption);
        setText("aircraft_thumbnail_modal_title", asset.title);
        setText("aircraft_thumbnail_modal_caption", asset.caption);
        container.classList.remove("hidden");
    }

    function showModal() {
        var modal = document.getElementById("aircraft_thumbnail_modal");
        if (!modal) {
            return;
        }
        modal.classList.remove("hidden");
        modal.setAttribute("aria-hidden", "false");
    }

    function hideModal() {
        var modal = document.getElementById("aircraft_thumbnail_modal");
        if (!modal) {
            return;
        }
        modal.classList.add("hidden");
        modal.setAttribute("aria-hidden", "true");
    }

    function bindThumbnailUI() {
        var button = document.getElementById("selected_aircraft_thumbnail_button");
        var modal = document.getElementById("aircraft_thumbnail_modal");
        var closeButton = document.getElementById("aircraft_thumbnail_modal_close");

        if (button) {
            button.addEventListener("click", showModal);
        }

        if (closeButton) {
            closeButton.addEventListener("click", hideModal);
        }

        if (modal) {
            modal.addEventListener("click", function(evt) {
                if (evt.target === modal) {
                    hideModal();
                }
            });
        }

        document.addEventListener("keydown", function(evt) {
            if (evt.key === "Escape") {
                hideModal();
            }
        });
    }

    var originalRefreshSelected = window.refreshSelected;
    window.refreshSelected = function() {
        var result = originalRefreshSelected.apply(this, arguments);
        updateTypeDisplays();
        updateThumbnail();
        return result;
    };

    var originalRefreshHighlighted = window.refreshHighlighted;
    if (typeof originalRefreshHighlighted === "function") {
        window.refreshHighlighted = function() {
            var result = originalRefreshHighlighted.apply(this, arguments);
            updateTypeDisplays();
            return result;
        };
    }

    var originalRefreshTableInfo = window.refreshTableInfo;
    if (typeof originalRefreshTableInfo === "function") {
        window.refreshTableInfo = function() {
            var result = originalRefreshTableInfo.apply(this, arguments);
            updateTypeDisplays();
            return result;
        };
    }

    window.addEventListener("load", function() {
        bindThumbnailUI();
        loadAutoCacheIndex().finally(function() {
            updateTypeDisplays();
            updateThumbnail();
        });
        updateTypeDisplays();
        updateThumbnail();
    });
})();
