#[derive(Debug, Clone, PartialEq)]
pub struct IngressBackendConfig {
    pub host: String,
    pub vip: String,
    pub port: u16,
    pub cert_path: String,
    pub key_path: String,
}

pub fn render_nginx_config(backends: &[IngressBackendConfig]) -> String {
    let mut config = String::from(
        "user www; worker_processes 1;\nevents { worker_connections 1024; }\nhttp {\n    \
         include /usr/local/etc/nginx/conf.d/*.conf;\n    server {\n        listen 80 default_server;\n        return 301 https://$host$request_uri;\n    }\n",
    );
    for backend in backends {
        config.push_str(&format!(
            "    server {{\n        listen 443 ssl;\n        server_name {host};\n        ssl_certificate {cert_path};\n        ssl_certificate_key {key_path};\n        location / {{\n            proxy_pass http://{vip}:{port};\n        }}\n    }}\n",
            host = backend.host,
            cert_path = backend.cert_path,
            key_path = backend.key_path,
            vip = backend.vip,
            port = backend.port,
        ));
    }
    config.push_str("}\n");
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(host: &str, vip: &str, port: u16) -> IngressBackendConfig {
        IngressBackendConfig {
            host: host.to_string(),
            vip: vip.to_string(),
            port,
            cert_path: format!("/usr/local/etc/nginx/certs/{host}.crt"),
            key_path: format!("/usr/local/etc/nginx/certs/{host}.key"),
        }
    }

    #[test]
    fn empty_backends_still_produces_a_valid_shaped_config_with_the_http_redirect() {
        let config = render_nginx_config(&[]);
        assert!(config.contains("listen 80 default_server;"));
        assert!(config.contains("return 301 https://$host$request_uri;"));
    }

    /// This project regenerates and rewrites the whole nginx.conf on every
    /// reconcile tick (`keel-agentd::reconciler::reconcile_ingress_config`),
    /// so any hand-maintained server block placed directly in this file
    /// would be silently wiped within seconds. This `include` is the one
    /// stable extension point: content dropped into
    /// `/usr/local/etc/nginx/conf.d/` (never written by this function)
    /// survives every future regeneration.
    #[test]
    fn includes_the_stable_conf_d_extension_point() {
        let config = render_nginx_config(&[]);
        assert!(
            config.contains("include /usr/local/etc/nginx/conf.d/*.conf;"),
            "got: {config}"
        );
    }

    #[test]
    fn one_backend_produces_one_server_block_with_proxy_pass_to_its_vip_and_port() {
        let config = render_nginx_config(&[backend("example.com", "10.0.0.9", 8080)]);
        assert!(config.contains("server_name example.com;"));
        assert!(config.contains("proxy_pass http://10.0.0.9:8080;"));
        assert!(config.contains("ssl_certificate /usr/local/etc/nginx/certs/example.com.crt;"));
        assert!(config.contains("ssl_certificate_key /usr/local/etc/nginx/certs/example.com.key;"));
    }

    #[test]
    fn multiple_backends_each_get_their_own_server_block() {
        let config = render_nginx_config(&[
            backend("a.example.com", "10.0.0.9", 8080),
            backend("b.example.com", "10.0.0.10", 9090),
        ]);
        assert!(config.contains("server_name a.example.com;"));
        assert!(config.contains("proxy_pass http://10.0.0.9:8080;"));
        assert!(config.contains("server_name b.example.com;"));
        assert!(config.contains("proxy_pass http://10.0.0.10:9090;"));
    }

    #[test]
    fn rendering_is_deterministic_for_the_same_input() {
        let backends = vec![backend("example.com", "10.0.0.9", 8080)];
        assert_eq!(
            render_nginx_config(&backends),
            render_nginx_config(&backends)
        );
    }
}
