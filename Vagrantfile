# -*- mode: ruby -*-
# vi: set ft=ruby :

require 'etc'
require 'pathname'
require File.join(File.dirname(__FILE__), "vagrant", "dependency_manager")

check_plugins ["vagrant-vbguest"]

Vagrant.configure("2") do |config|
    config.vm.synced_folder ".", "/home/vagrant/domain-intel"
    config.vm.network "private_network", ip: "10.200.10.1"

    # Kafka/ZooKeeper ports.
    config.vm.network "forwarded_port", guest: 9092, host: 9092
    config.vm.network "forwarded_port", guest: 2181, host: 2181

    # ArangoDB ports.
    config.vm.network "forwarded_port", guest: 8529, host: 8529

    # SonarQube ports.
    config.vm.network "forwarded_port", guest: 9000, host: 9000
    config.vm.network "forwarded_port", guest: 9002, host: 9002

    config.vm.define 'domain-intel' do |domain_intel|
        domain_intel.vm.provider "virtualbox"
        domain_intel.vm.box = "ubuntu/trusty64"

        config.vm.provider "virtualbox" do |v|
            v.memory = 4096
            v.cpus = 2
        end

        config.ssh.forward_agent = true
        config.ssh.insert_key = false

        # Provision with Ansible.
        config.vm.provision 'domain-intel', type: "ansible" do |ansible|
            ansible.playbook = "../ansible/app-domain-intel-services.yml"

            # Group => server
            ansible.groups = {
                "app-domain-intel-services" => ["domain-intel"]
            }

            ansible.verbose = "v"
            if ENV['ANSIBLE_VERBOSE']
                ansible.verbose = ENV['ANSIBLE_VERBOSE']
            end

            if ENV['ANSIBLE_TAGS']
                ansible.tags = ENV['ANSIBLE_TAGS']
            end

            ansible.extra_vars = {
                ephemeral_testing: 1,
                code_analysis: 1,
                app_domain_intel: {
                    branch: 'master',
                    ipe_env: 'development',
                }
            }
        end
    end
end
