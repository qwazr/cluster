node {

    stage 'Checkout'

    git url: 'https://github.com/qwazr/cluster.git'

    stage 'Build'

    withMaven(maven: 'Maven') {
        sh "mvn -U clean deploy"
    }
}
