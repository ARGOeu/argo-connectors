FROM rockylinux:9
LABEL org.opencontainers.image.authors=dvrcic@srce.hr
RUN groupadd user -g 1000 && useradd -u 1000 -g 1000 user -m -d /home/user -s /bin/zsh
ADD https://repository.egi.eu/sw/production/cas/1/current/repo-files/egi-trustanchors.repo /etc/yum.repos.d/
RUN dnf -y update; dnf clean all
RUN dnf -y install epel-release rocky-repos && \
    dnf -y install \
    ack \
    ca-policy-egi-core \
    ctags \
    fortune-mod \
    gcc \
    git \
    glibc-all-langpacks \
    iproute \
    make \
    mc \
    net-tools \
    openldap-devel \
    passwd \
    procps-ng \
    python3-devel \
    python3-pip \
    python3-setuptools \
    rsync \
    rpmdevtools \
    sudo \
    the_silver_searcher \
    tmux \
    vim \
    wget \
    yum-utils \
    zsh
RUN dnf --enablerepo=devel install -y ncurses-compat-libs
RUN rm -rf /etc/pki/ca-trust/source/anchors/*; \
    cp /etc/grid-security/certificates/*.pem /etc/pki/ca-trust/source/anchors
ADD https://cacerts.digicert.com/TERENAeScienceSSLCA3.crt.pem /etc/pki/ca-trust/source/anchors
COPY DigiCertCA.crt GEANT_OV_RSA_CA_4.pem /etc/pki/ca-trust/source/anchors
RUN update-ca-trust
RUN pip install -U pip; pip3 install -U pip
RUN python3.9 -m venv /opt/poetry-tool/
RUN sh -c '\
  . /opt/poetry-tool/bin/activate \
  && pip install -U pip \
  && pip install -U setuptools==69.2.0 \
  && pip install -U poetry'
RUN ln -s /opt/poetry-tool/bin/poetry /usr/local/bin/
RUN python3.9 -m venv /opt/argo-connectors/
RUN sh -c '\
  . /opt/argo-connectors/bin/activate \
  && pip install -U pip'
RUN chown -R user:user /opt/argo-connectors
RUN passwd -d root
VOLUME [ "/sys/fs/cgroup" ]
RUN echo "user ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN echo "Defaults	env_keep += \"PYTHONPATH\"" >> /etc/sudoers
USER user
COPY chezmoi-config-apply.sh /home/user
WORKDIR /home/user
RUN mkdir /home/user/.ssh/ && sudo chmod 700 /home/user/.ssh && sudo chown -R user:user /home/user/.ssh
RUN curl -sfL https://git.io/chezmoi | sh
CMD ["/bin/zsh"]
