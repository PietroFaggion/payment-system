package com.pietrofaggion.paymentsystem.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    /**
     * Defines the HTTP security filter chain:
     * <ul>
     *   <li><b>CSRF disabled</b> — the API is stateless and consumed by machine clients
     *       that do not use browser sessions.</li>
     *   <li><b>Stateless sessions</b> — no {@code HttpSession} is created; each request
     *       must carry credentials.</li>
     *   <li><b>Open paths</b> — {@code /actuator/health}, {@code /actuator/info}, and all
     *       Swagger UI / OpenAPI paths are accessible without authentication.</li>
     *   <li><b>All other paths</b> require valid HTTP Basic credentials.</li>
     * </ul>
     * Credentials are configured via {@code spring.security.user.name/password}
     * (overridable with the {@code SECURITY_USER} / {@code SECURITY_PASSWORD} environment variables).
     *
     * @param http the {@link HttpSecurity} builder provided by Spring Security
     * @return the configured {@link SecurityFilterChain}
     */
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(
                                "/actuator/health",
                                "/actuator/info",
                                "/swagger-ui/**",
                                "/swagger-ui.html",
                                "/v3/api-docs/**"
                        ).permitAll()
                        .anyRequest().authenticated()
                )
                .httpBasic(Customizer.withDefaults());

        return http.build();
    }
}
