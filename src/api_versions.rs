const API_VERSIONS: &[(i16, (i16, i16))] = &[
    (1, (16, 16)),
    (18, (0, 4)),
    (75, (0, 0)),
];

pub fn get_all_apis() -> &'static [(i16, (i16, i16))] {
    API_VERSIONS
}

pub fn get_supported_api_versions(api_key: i16) -> Option<(i16, i16)> {
    API_VERSIONS.iter().find_map(|&(key, version)| {
        if key == api_key {
            Some(version)
        } else {
            None
        }
    })
}
