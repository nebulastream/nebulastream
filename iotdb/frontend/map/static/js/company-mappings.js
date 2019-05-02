const companyMappings = {

    processCompanyResult: function (companyResult) {
        if (companyResult.locationLatLon && companyResult.locationLatLon.length > 0) {
            companyResult.displayLocation = {
                latitude:  companyResult.locationLatLon[0].split(",")[0],
                longitude: companyResult.locationLatLon[0].split(",")[1]
            };
        }

        if (Array.isArray(companyResult.nameDe) && companyResult.nameDe.length > 0) {
            companyResult.companyName = companyResult.nameDe[0];
        } else if (Array.isArray(companyResult.nameEn) && companyResult.nameEn.length > 0) {
            companyResult.companyName = companyResult.nameEn[0];
        } else {
            companyResult.companyName = "No name data"
        }

        return companyResult;
    },

};

